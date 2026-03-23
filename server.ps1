$ErrorActionPreference = 'Stop'

$HostName = '127.0.0.1'
$Port = 3000
$ImsBase = 'https://ims.gov.il'
$RadarEndpoint = "$ImsBase/radar_satellite/1"
$ImsRequestTimeoutSeconds = 45
$RadarRefreshSeconds = 60
$LightningEndpoint = 'https://www.freelightning.com/glm/glm(Flashes).php'
$LightningRequestTimeoutSeconds = 4
$PublicDir = Join-Path $PSScriptRoot 'public'
$RadarCacheTtlSeconds = 60
$RadarFrameStepMinutes = 5
$RadarHistoryHours = 6
$RadarDesiredFrameCount = [int](($RadarHistoryHours * 60) / $RadarFrameStepMinutes) + 1
$script:RadarCacheFrames = @()
$script:RadarCacheFetchedAt = $null
$script:LastRadarRefreshAttemptAt = $null
$script:LastSuccessfulRadarFetchAt = $null
$script:RadarColorScale = $null
$RadarFramesCachePath = Join-Path $PSScriptRoot 'radar-frames-cache.json'
$RadarFallbackImagePath = Join-Path $PSScriptRoot 'ims-latest.png'
$RadarImageCacheTtlSeconds = 600
$script:RadarImageCache = @{}
$script:LatestRadarImageCache = $null
$LightningCacheTtlSeconds = 40
$script:LightningCacheRaw = $null
$script:LightningCacheFetchedAt = $null
$LightningHistoryPath = Join-Path $PSScriptRoot 'lightning-history.json'
$LightningHistoryRetentionHours = 24
$LightningPollSeconds = 40
$script:LightningHistory = @()
$script:LastLightningPollAt = $null
$script:LastLightningPollAttemptAt = $null

$MimeTypes = @{
  '.css' = 'text/css; charset=utf-8'
  '.html' = 'text/html; charset=utf-8'
  '.js' = 'application/javascript; charset=utf-8'
  '.json' = 'application/json; charset=utf-8'
  '.png' = 'image/png'
  '.svg' = 'image/svg+xml'
}

$DefaultRadarColorScale = [pscustomobject]@{
  source = 'default'
  unit = 'mm/h'
  title = 'mm/h'
  entries = @(
    [pscustomobject]@{ thresholdMmPerHour = 0.1; colorHex = '#0165ef' }
    [pscustomobject]@{ thresholdMmPerHour = 0.2; colorHex = '#00c3c9' }
    [pscustomobject]@{ thresholdMmPerHour = 0.7; colorHex = '#00a09b' }
    [pscustomobject]@{ thresholdMmPerHour = 1.2; colorHex = '#008c4d' }
    [pscustomobject]@{ thresholdMmPerHour = 2.0; colorHex = '#00b02c' }
    [pscustomobject]@{ thresholdMmPerHour = 4.0; colorHex = '#01d31c' }
    [pscustomobject]@{ thresholdMmPerHour = 6.0; colorHex = '#12f218' }
    [pscustomobject]@{ thresholdMmPerHour = 9.0; colorHex = '#7cff21' }
    [pscustomobject]@{ thresholdMmPerHour = 13.0; colorHex = '#fefd19' }
    [pscustomobject]@{ thresholdMmPerHour = 18.0; colorHex = '#ffcf00' }
    [pscustomobject]@{ thresholdMmPerHour = 24.0; colorHex = '#ffa800' }
    [pscustomobject]@{ thresholdMmPerHour = 30.0; colorHex = '#ff7d01' }
    [pscustomobject]@{ thresholdMmPerHour = 40.0; colorHex = '#fb3f00' }
    [pscustomobject]@{ thresholdMmPerHour = 50.0; colorHex = '#e10a12' }
    [pscustomobject]@{ thresholdMmPerHour = 100.0; colorHex = '#d00078' }
    [pscustomobject]@{ thresholdMmPerHour = 200.0; colorHex = '#ff00fe' }
  )
}

function Get-ReasonPhrase {
  param([int] $StatusCode)

  switch ($StatusCode) {
    200 { 'OK' }
    400 { 'Bad Request' }
    403 { 'Forbidden' }
    404 { 'Not Found' }
    405 { 'Method Not Allowed' }
    500 { 'Internal Server Error' }
    502 { 'Bad Gateway' }
    default { 'OK' }
  }
}

function Send-Response {
  param(
    [Parameter(Mandatory = $true)] [System.Net.Sockets.NetworkStream] $Stream,
    [Parameter(Mandatory = $true)] [int] $StatusCode,
    [Parameter(Mandatory = $true)] [byte[]] $Body,
    [Parameter(Mandatory = $true)] [string] $ContentType,
    [string] $CacheControl = 'no-store'
  )

  $reason = Get-ReasonPhrase -StatusCode $StatusCode
  $headerText = @(
    "HTTP/1.1 $StatusCode $reason"
    "Content-Type: $ContentType"
    "Content-Length: $($Body.Length)"
    "Cache-Control: $CacheControl"
    'Connection: close'
    ''
    ''
  ) -join "`r`n"

  $headerBytes = [System.Text.Encoding]::ASCII.GetBytes($headerText)
  $Stream.Write($headerBytes, 0, $headerBytes.Length)
  $Stream.Write($Body, 0, $Body.Length)
  $Stream.Flush()
}

function Send-Json {
  param(
    [System.Net.Sockets.NetworkStream] $Stream,
    [int] $StatusCode,
    $Payload
  )

  $json = $Payload | ConvertTo-Json -Depth 8
  $body = [System.Text.Encoding]::UTF8.GetBytes($json)
  Send-Response -Stream $Stream -StatusCode $StatusCode -Body $body -ContentType 'application/json; charset=utf-8'
}

function Get-FrameLabel {
  param([object] $Frame, [int] $Index)

  if ($Frame.PSObject.Properties.Name -contains 'forecast_time' -and $Frame.forecast_time) {
    return [string] $Frame.forecast_time
  }

  if ($Frame.PSObject.Properties.Name -contains 'valid_time' -and $Frame.valid_time) {
    return [string] $Frame.valid_time
  }

  return "Frame $($Index + 1)"
}

function Get-FallbackRadarFrames {
  if (-not (Test-Path -LiteralPath $RadarFramesCachePath -PathType Leaf)) {
    return @()
  }

  try {
    $raw = Get-Content -LiteralPath $RadarFramesCachePath -Raw
    if (-not $raw.Trim()) {
      return @()
    }

    $loaded = ConvertFrom-Json $raw
    if ($loaded -is [System.Array]) {
      return @($loaded)
    }

    if ($loaded) {
      return @($loaded)
    }
  } catch {
  }

  return @()
}

function Save-RadarFramesCache {
  param([object[]] $Frames)

  try {
    $Frames | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $RadarFramesCachePath -Encoding UTF8
  } catch {
  }
}

function Get-LocalFallbackFrame {
  if (-not (Test-Path -LiteralPath $RadarFallbackImagePath -PathType Leaf)) {
    return @()
  }

  return @(
    [pscustomobject]@{
      id = 'local-fallback-radar'
      label = 'Cached local radar'
      forecastTime = $null
      upstreamPath = '/__local__/ims-latest.png'
      imageUrl = '/api/fallback-image'
    }
  )
}

function Normalize-HexColor {
  param([string] $Color)

  if (-not $Color) {
    return $null
  }

  $trimmed = $Color.Trim()
  if (-not $trimmed.StartsWith('#')) {
    $trimmed = "#$trimmed"
  }

  return $trimmed.ToLowerInvariant()
}

function Get-RadarSourceInfo {
  $fallbackUpdatedAt = $null
  if (Test-Path -LiteralPath $RadarFallbackImagePath -PathType Leaf) {
    try {
      $fallbackUpdatedAt = (Get-Item -LiteralPath $RadarFallbackImagePath).LastWriteTimeUtc.ToString('o')
    } catch {
      $fallbackUpdatedAt = $null
    }
  }

  $isFallbackOnly = $script:RadarCacheFrames.Count -gt 0 -and $script:RadarCacheFrames.Count -eq 1 -and $script:RadarCacheFrames[0].id -eq 'local-fallback-radar'
  $hasCachedFrames = $script:RadarCacheFrames.Count -gt 0
  $isFresh = Test-RadarFramesCacheIsFresh

  return @{
    mode = if ($isFallbackOnly) { 'fallback' } elseif ($hasCachedFrames -and -not $isFresh) { 'stale-cache' } else { 'live' }
    lastSuccessfulRadarFetchAt = if ($script:LastSuccessfulRadarFetchAt) { $script:LastSuccessfulRadarFetchAt.ToUniversalTime().ToString('o') } else { $null }
    fallbackUpdatedAt = $fallbackUpdatedAt
    updatedAt = if ($script:RadarCacheFetchedAt) { $script:RadarCacheFetchedAt.ToUniversalTime().ToString('o') } else { $fallbackUpdatedAt }
  }
}

function ConvertTo-RadarColorScale {
  param([object] $Colorbar)

  if (-not $Colorbar -or -not $Colorbar.items) {
    return $null
  }

  $entries = @()

  foreach ($property in $Colorbar.items.PSObject.Properties) {
    $item = $property.Value
    if (-not $item) {
      continue
    }

    $threshold = 0.0
    if (-not [double]::TryParse([string] $item.mark, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $threshold)) {
      continue
    }

    $colorHex = Normalize-HexColor -Color ([string] $item.color)
    if (-not $colorHex) {
      continue
    }

    $entries += [pscustomobject]@{
      thresholdMmPerHour = $threshold
      colorHex = $colorHex
    }
  }

  if ($entries.Count -eq 0) {
    return $null
  }

  return [pscustomobject]@{
    source = 'ims'
    unit = 'mm/h'
    title = if ($Colorbar.title) { [string] $Colorbar.title } else { 'mm/h' }
    entries = @($entries | Sort-Object thresholdMmPerHour)
  }
}

function Get-RadarColorScale {
  if ($script:RadarColorScale) {
    return $script:RadarColorScale
  }

  return $DefaultRadarColorScale
}

function Expand-RadarItems {
  param([object[]] $Items)

  $frames = @(
    $Items | Where-Object { $_ -and $_.file_name }
  )

  if ($frames.Count -eq 0) {
    return @()
  }

  if ($frames.Count -ge $RadarDesiredFrameCount) {
    return @($frames | Select-Object -Last $RadarDesiredFrameCount)
  }

  $firstFrame = $frames[0]
  $match = [regex]::Match([string] $firstFrame.file_name, '^(.*/IMSRadar4GIS_)(\d{12})(_0\.png)$')
  if (-not $match.Success) {
    return @($frames)
  }

  $prefix = $match.Groups[1].Value
  $timestamp = $match.Groups[2].Value
  $suffix = $match.Groups[3].Value
  $baseTime = [datetime]::ParseExact($timestamp, 'yyyyMMddHHmm', [System.Globalization.CultureInfo]::InvariantCulture)
  $missingCount = $RadarDesiredFrameCount - $frames.Count
  $syntheticFrames = @()

  for ($i = $missingCount; $i -ge 1; $i--) {
    $frameTime = $baseTime.AddMinutes(-$RadarFrameStepMinutes * $i)
    $syntheticFrames += [pscustomobject]@{
      file_name = "$prefix$($frameTime.ToString('yyyyMMddHHmm'))$suffix"
      forecast_time = $frameTime.ToString('yyyy-MM-dd HH:mm:ss')
      isSynthetic = $true
    }
  }

  return @($syntheticFrames + $frames)
}

function ConvertTo-RadarFrames {
  param([object[]] $Items)

  $expandedItems = @(Expand-RadarItems -Items $Items)
  $frames = @(
    for ($i = 0; $i -lt $expandedItems.Count; $i++) {
      $frame = $expandedItems[$i]
      if (-not $frame.file_name) {
        continue
      }

      [pscustomobject]@{
        id = "$i-$([System.IO.Path]::GetFileName([string] $frame.file_name))"
        label = Get-FrameLabel -Frame $frame -Index $i
        forecastTime = if ($frame.PSObject.Properties.Name -contains 'forecast_time') { $frame.forecast_time } else { $null }
        upstreamPath = [string] $frame.file_name
        imageUrl = "/api/image?path=$([uri]::EscapeDataString([string] $frame.file_name))&seek=$(if ($frame.PSObject.Properties.Name -contains 'isSynthetic' -and $frame.isSynthetic) { 'forward' } else { 'backward' })"
      }
    }
  )

  return $frames
}

function Initialize-RadarFrames {
  if ($script:RadarCacheFrames.Count -gt 0) {
    return
  }

  if (-not $script:RadarColorScale) {
    $script:RadarColorScale = $DefaultRadarColorScale
  }

  $fileCacheFrames = @(Get-FallbackRadarFrames)
  if ($fileCacheFrames.Count -gt 0) {
    $script:RadarCacheFrames = @($fileCacheFrames)
    try {
      $script:RadarCacheFetchedAt = (Get-Item -LiteralPath $RadarFramesCachePath).LastWriteTime
    } catch {
      $script:RadarCacheFetchedAt = $null
    }
    return
  }

  $localFallbackFrames = @(Get-LocalFallbackFrame)
  if ($localFallbackFrames.Count -gt 0) {
    $script:RadarCacheFrames = @($localFallbackFrames)
  }
}

function Test-RadarFramesCacheIsFresh {
  if ($script:RadarCacheFrames.Count -eq 0 -or -not $script:RadarCacheFetchedAt) {
    return $false
  }

  return (((Get-Date) - $script:RadarCacheFetchedAt).TotalSeconds -lt $RadarCacheTtlSeconds)
}

function Update-RadarFrames {
  $script:LastRadarRefreshAttemptAt = Get-Date

  $payload = Invoke-WebRequest -UseBasicParsing -TimeoutSec $ImsRequestTimeoutSeconds $RadarEndpoint | Select-Object -ExpandProperty Content | ConvertFrom-Json
  $items = @()

  if ($payload.data -and $payload.data.types -and $payload.data.types.IMSRadar) {
    $items = @($payload.data.types.IMSRadar)
  }

  $colorScale = ConvertTo-RadarColorScale -Colorbar $payload.radar_colorbar
  if ($colorScale) {
    $script:RadarColorScale = $colorScale
  }

  $frames = @(ConvertTo-RadarFrames -Items $items)
  if ($frames.Count -eq 0) {
    throw 'IMS radar feed returned no frames.'
  }

  $script:RadarCacheFrames = @($frames)
  $script:RadarCacheFetchedAt = Get-Date
  $script:LastSuccessfulRadarFetchAt = $script:RadarCacheFetchedAt
  Save-RadarFramesCache -Frames $script:RadarCacheFrames
  return @($script:RadarCacheFrames)
}

function Get-RadarFrames {
  Initialize-RadarFrames

  if (Test-RadarFramesCacheIsFresh) {
    return @($script:RadarCacheFrames)
  }

  try {
    return @(Update-RadarFrames)
  } catch {
    if ($script:RadarCacheFrames.Count -gt 0) {
      return @($script:RadarCacheFrames)
    }

    $localFallbackFrames = @(Get-LocalFallbackFrame)
    if ($localFallbackFrames.Count -gt 0) {
      $script:RadarCacheFrames = @($localFallbackFrames)
      return @($script:RadarCacheFrames)
    }

    throw
  }
}

function Get-CachedRadarImage {
  param([string] $PathValue)

  if (-not $script:RadarImageCache.ContainsKey($PathValue)) {
    return $null
  }

  $entry = $script:RadarImageCache[$PathValue]
  if (-not $entry -or -not $entry.fetchedAt) {
    return $null
  }

  $hasUsableImageBytes = $entry.bytes -and (
    (($entry.contentType -is [string]) -and $entry.contentType.StartsWith('image/', [System.StringComparison]::OrdinalIgnoreCase)) -or
    (Test-IsPngBytes -Bytes $entry.bytes)
  )
  if (-not $hasUsableImageBytes) {
    $script:RadarImageCache.Remove($PathValue)
    return $null
  }

  $ageSeconds = ((Get-Date) - $entry.fetchedAt).TotalSeconds
  if ($ageSeconds -gt $RadarImageCacheTtlSeconds) {
    $script:RadarImageCache.Remove($PathValue)
    return $null
  }

  return $entry
}

function Set-CachedRadarImage {
  param(
    [string] $PathValue,
    [byte[]] $Bytes,
    [string] $ContentType
  )

  $entry = @{
    path = $PathValue
    bytes = $Bytes
    contentType = $ContentType
    fetchedAt = Get-Date
  }

  $script:RadarImageCache[$PathValue] = $entry
  $script:LatestRadarImageCache = $entry

  try {
    [System.IO.File]::WriteAllBytes($RadarFallbackImagePath, $Bytes)
  } catch {
  }

  return $entry
}

function Get-StaticFile {
  param([string] $RequestPath)

  $localPath = if ($RequestPath -eq '/') { '/index.html' } else { $RequestPath }
  $relative = $localPath.TrimStart('/').Replace('/', [System.IO.Path]::DirectorySeparatorChar)
  $targetPath = [System.IO.Path]::GetFullPath((Join-Path $PublicDir $relative))
  $publicRoot = [System.IO.Path]::GetFullPath($PublicDir)

  if (-not $targetPath.StartsWith($publicRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw [System.UnauthorizedAccessException]::new('Forbidden')
  }

  if (-not (Test-Path -LiteralPath $targetPath -PathType Leaf)) {
    throw [System.IO.FileNotFoundException]::new('Not found')
  }

  $ext = [System.IO.Path]::GetExtension($targetPath).ToLowerInvariant()
  return @{
    Bytes = [System.IO.File]::ReadAllBytes($targetPath)
    ContentType = if ($MimeTypes.ContainsKey($ext)) { $MimeTypes[$ext] } else { 'application/octet-stream' }
  }
}

function Parse-RequestTarget {
  param([string] $Target)

  $uri = [System.Uri]::new("http://localhost$Target")
  $query = @{}

  if ($uri.Query.Length -gt 1) {
    foreach ($pair in $uri.Query.TrimStart('?').Split('&', [System.StringSplitOptions]::RemoveEmptyEntries)) {
      $parts = $pair.Split('=', 2)
      $key = [uri]::UnescapeDataString($parts[0])
      $value = if ($parts.Count -gt 1) { [uri]::UnescapeDataString($parts[1]) } else { '' }
      $query[$key] = $value
    }
  }

  return @{
    Path = $uri.AbsolutePath
    Query = $query
  }
}

function Get-QueryDouble {
  param(
    [hashtable] $Query,
    [string] $Name,
    [double] $Fallback
  )

  $raw = $Query[$Name]
  if ($null -eq $raw -or $raw -eq '') {
    return $Fallback
  }

  $parsed = 0.0
  if ([double]::TryParse($raw, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $parsed)) {
    return $parsed
  }

  return $Fallback
}

function Read-LightningFeedRaw {
  $now = Get-Date
  $cacheIsFresh = $script:LightningCacheRaw -and $script:LightningCacheFetchedAt -and (($now - $script:LightningCacheFetchedAt).TotalSeconds -lt $LightningCacheTtlSeconds)

  if ($cacheIsFresh) {
    return $script:LightningCacheRaw
  } else {
    try {
      $content = Invoke-WebRequest -UseBasicParsing -TimeoutSec $LightningRequestTimeoutSeconds $LightningEndpoint | Select-Object -ExpandProperty Content
      $script:LightningCacheRaw = $content
      $script:LightningCacheFetchedAt = $now
      return $content
    } catch {
      if ($script:LightningCacheRaw) {
        return $script:LightningCacheRaw
      } else {
        throw
      }
    }
  }
}

function Parse-LightningFeed {
  param([string] $Content)

  $matches = [regex]::Matches($Content, '<flash>([-0-9.]+),([-0-9.]+)</flash>')
  $flashes = @()

  foreach ($match in $matches) {
    $flashes += [pscustomobject]@{
      lat = [double]::Parse($match.Groups[1].Value, [System.Globalization.CultureInfo]::InvariantCulture)
      lon = [double]::Parse($match.Groups[2].Value, [System.Globalization.CultureInfo]::InvariantCulture)
    }
  }

  return @($flashes)
}

function Save-LightningHistory {
  $script:LightningHistory | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $LightningHistoryPath -Encoding UTF8
}

function Load-LightningHistory {
  if (-not (Test-Path -LiteralPath $LightningHistoryPath -PathType Leaf)) {
    $script:LightningHistory = @()
    return
  }

  $raw = Get-Content -LiteralPath $LightningHistoryPath -Raw
  if (-not $raw.Trim()) {
    $script:LightningHistory = @()
    return
  }

  $loaded = ConvertFrom-Json $raw
  if ($loaded -is [System.Array]) {
    $script:LightningHistory = @($loaded)
  } elseif ($loaded) {
    $script:LightningHistory = @($loaded)
  } else {
    $script:LightningHistory = @()
  }
}

function Prune-LightningHistory {
  $cutoff = (Get-Date).ToUniversalTime().AddHours(-$LightningHistoryRetentionHours)
  $script:LightningHistory = @(
    $script:LightningHistory | Where-Object {
      [datetime]::Parse($_.observedAt, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind) -ge $cutoff
    }
  )
}

function Update-LightningHistory {
  $now = (Get-Date).ToUniversalTime()
  $script:LastLightningPollAttemptAt = $now
  $content = Read-LightningFeedRaw
  $flashes = Parse-LightningFeed -Content $content
  $recentCutoff = $now.AddMinutes(-10)

  foreach ($flash in $flashes) {
    $existing = $script:LightningHistory | Where-Object {
      $_.lat -eq $flash.lat -and $_.lon -eq $flash.lon -and
      [datetime]::Parse($_.observedAt, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind) -ge $recentCutoff
    } | Select-Object -First 1

    if (-not $existing) {
      $script:LightningHistory += [pscustomobject]@{
        lat = $flash.lat
        lon = $flash.lon
        observedAt = $now.ToString('o')
      }
    }
  }

  Prune-LightningHistory
  Save-LightningHistory
  $script:LastLightningPollAt = $now
}

function Get-LightningPayload {
  param([hashtable] $Query)

  $minLat = Get-QueryDouble -Query $Query -Name 'minLat' -Fallback 29.85
  $maxLat = Get-QueryDouble -Query $Query -Name 'maxLat' -Fallback 34.17
  $minLon = Get-QueryDouble -Query $Query -Name 'minLon' -Fallback 32.28
  $maxLon = Get-QueryDouble -Query $Query -Name 'maxLon' -Fallback 37.38
  $windowMins = [int](Get-QueryDouble -Query $Query -Name 'windowMins' -Fallback 5)
  $frameTimeRaw = $Query['frameTime']
  $frameTime = $null

  if ($frameTimeRaw) {
    try {
      $frameTime = [datetime]::Parse($frameTimeRaw, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AssumeUniversal)
      $frameTime = $frameTime.ToUniversalTime()
    } catch {
      $frameTime = $null
    }
  }

  if (-not $script:LastLightningPollAttemptAt -or (((Get-Date).ToUniversalTime() - $script:LastLightningPollAttemptAt).TotalSeconds -ge $LightningPollSeconds)) {
    Update-LightningHistory
  }

  $flashes = @()
  $windowStart = if ($frameTime) { $frameTime.AddMinutes(-$windowMins) } else { (Get-Date).ToUniversalTime().AddMinutes(-$windowMins) }
  $windowEnd = if ($frameTime) { $frameTime } else { (Get-Date).ToUniversalTime() }

  foreach ($flash in $script:LightningHistory) {
    $observedAt = [datetime]::Parse($flash.observedAt, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind)
    $lat = [double]$flash.lat
    $lon = [double]$flash.lon

    if ($observedAt -lt $windowStart -or $observedAt -gt $windowEnd) {
      continue
    }

    if ($lat -lt $minLat -or $lat -gt $maxLat -or $lon -lt $minLon -or $lon -gt $maxLon) {
      continue
    }

    $flashes += [pscustomobject]@{
      lat = $lat
      lon = $lon
      observedAt = $flash.observedAt
    }
  }

  return @{
    source = $LightningEndpoint
    updatedAt = (Get-Date).ToUniversalTime().ToString('o')
    count = $flashes.Count
    windowMins = $windowMins
    frameTime = if ($frameTime) { $frameTime.ToString('o') } else { $null }
    flashes = $flashes
  }
}

function Get-ShiftedRadarPath {
  param(
    [string] $PathValue,
    [int] $OffsetSteps = 0
  )

  $match = [regex]::Match($PathValue, '^(.*/IMSRadar4GIS_)(\d{12})(_0\.png)$')
  if (-not $match.Success) {
    return $null
  }

  $prefix = $match.Groups[1].Value
  $timestamp = $match.Groups[2].Value
  $suffix = $match.Groups[3].Value
  $dt = [datetime]::ParseExact($timestamp, 'yyyyMMddHHmm', [System.Globalization.CultureInfo]::InvariantCulture)

  $shifted = $dt.AddMinutes($RadarFrameStepMinutes * $OffsetSteps).ToString('yyyyMMddHHmm')
  return "$prefix$shifted$suffix"
}

function ConvertTo-ResponseBytes {
  param($Content)

  if ($Content -is [byte[]]) {
    return $Content
  }

  if ($Content -is [string]) {
    return [System.Text.Encoding]::UTF8.GetBytes($Content)
  }

  return [byte[]] $Content
}

function Test-IsPngBytes {
  param([byte[]] $Bytes)

  if (-not $Bytes -or $Bytes.Length -lt 8) {
    return $false
  }

  return (
    $Bytes[0] -eq 137 -and
    $Bytes[1] -eq 80 -and
    $Bytes[2] -eq 78 -and
    $Bytes[3] -eq 71 -and
    $Bytes[4] -eq 13 -and
    $Bytes[5] -eq 10 -and
    $Bytes[6] -eq 26 -and
    $Bytes[7] -eq 10
  )
}

function Handle-Request {
  param(
    [string] $Method,
    [string] $Target,
    [System.Net.Sockets.NetworkStream] $Stream
  )

  if ($Method -ne 'GET') {
    Send-Json -Stream $Stream -StatusCode 405 -Payload @{ error = 'Only GET is supported' }
    return
  }

  $request = Parse-RequestTarget -Target $Target

  switch ($request.Path) {
    '/api/radar' {
      try {
        $frames = @(Get-RadarFrames)
        $sourceInfo = Get-RadarSourceInfo
        Send-Json -Stream $Stream -StatusCode 200 -Payload @{
          source = $RadarEndpoint
          updatedAt = if ($sourceInfo.updatedAt) { $sourceInfo.updatedAt } else { (Get-Date).ToUniversalTime().ToString('o') }
          mode = $sourceInfo.mode
          lastSuccessfulRadarFetchAt = $sourceInfo.lastSuccessfulRadarFetchAt
          fallbackUpdatedAt = $sourceInfo.fallbackUpdatedAt
          colorScale = Get-RadarColorScale
          frameCount = $frames.Count
          latestFrame = if ($frames.Count -gt 0) { $frames[-1] } else { $null }
          frames = $frames
        }
      } catch {
        Send-Json -Stream $Stream -StatusCode 502 -Payload @{
          error = 'Failed to fetch IMS radar feed'
          detail = $_.Exception.Message
        }
      }
      return
    }
    '/api/image' {
      $pathValue = $request.Query['path']
      if (-not $pathValue -or -not $pathValue.StartsWith('/')) {
        Send-Json -Stream $Stream -StatusCode 400 -Payload @{ error = 'Missing or invalid image path' }
        return
      }

      $seekMode = if ($request.Query['seek'] -eq 'forward') { 'forward' } else { 'backward' }
      $seekDirection = if ($seekMode -eq 'forward') { 1 } else { -1 }

      $cachedImage = Get-CachedRadarImage -PathValue $pathValue
      if ($cachedImage) {
        Send-Response -Stream $Stream -StatusCode 200 -Body $cachedImage.bytes -ContentType $cachedImage.contentType -CacheControl 'public, max-age=60'
        return
      }

      try {
        $candidatePath = $pathValue
        $resultBytes = $null
        $contentType = 'image/png'

        for ($attempt = 0; $attempt -lt $RadarDesiredFrameCount; $attempt++) {
          if ($attempt -eq 0) {
            $candidatePath = $pathValue
          } else {
            $candidatePath = Get-ShiftedRadarPath -PathValue $pathValue -OffsetSteps ($seekDirection * $attempt)
          }

          if (-not $candidatePath) {
            break
          }

          $upstreamUrl = [uri]::new([uri] $ImsBase, $candidatePath).AbsoluteUri

          try {
            $result = Invoke-WebRequest -UseBasicParsing -Uri $upstreamUrl -TimeoutSec $ImsRequestTimeoutSeconds
            $candidateBytes = ConvertTo-ResponseBytes -Content $result.Content
            $candidateContentType = if ($result.Headers.'Content-Type') { [string] $result.Headers.'Content-Type' } else { 'image/png' }
            $looksLikeImage = (
              $candidateContentType.StartsWith('image/', [System.StringComparison]::OrdinalIgnoreCase) -or
              (Test-IsPngBytes -Bytes $candidateBytes)
            )

            if (-not $looksLikeImage) {
              continue
            }

            $resultBytes = $candidateBytes
            $contentType = $candidateContentType
            break
          } catch {
            $response = $_.Exception.Response
            $is404 = $response -and [int]$response.StatusCode -eq 404

            if (-not $is404) {
              throw
            }
          }
        }

        if (-not $resultBytes) {
          throw 'No IMS radar image was available after fallback attempts.'
        }

        $cacheEntry = Set-CachedRadarImage -PathValue $pathValue -Bytes $resultBytes -ContentType $contentType
        if ($candidatePath -ne $pathValue) {
          Set-CachedRadarImage -PathValue $candidatePath -Bytes $resultBytes -ContentType $contentType | Out-Null
        }

        Send-Response -Stream $Stream -StatusCode 200 -Body $cacheEntry.bytes -ContentType $cacheEntry.contentType -CacheControl 'public, max-age=60'
      } catch {
        $cachedImage = Get-CachedRadarImage -PathValue $pathValue
        if (-not $cachedImage) {
          $cachedImage = $script:LatestRadarImageCache
        }

        if ($cachedImage) {
          Send-Response -Stream $Stream -StatusCode 200 -Body $cachedImage.bytes -ContentType $cachedImage.contentType -CacheControl 'public, max-age=30, stale-if-error=300'
        } elseif (Test-Path -LiteralPath $RadarFallbackImagePath -PathType Leaf) {
          $bytes = [System.IO.File]::ReadAllBytes($RadarFallbackImagePath)
          Send-Response -Stream $Stream -StatusCode 200 -Body $bytes -ContentType 'image/png' -CacheControl 'public, max-age=30, stale-if-error=300'
        } else {
          Send-Json -Stream $Stream -StatusCode 502 -Payload @{
            error = 'Failed to fetch IMS radar image'
            detail = $_.Exception.Message
          }
        }
      }
      return
    }
    '/api/fallback-image' {
      if (-not (Test-Path -LiteralPath $RadarFallbackImagePath -PathType Leaf)) {
        Send-Json -Stream $Stream -StatusCode 404 -Payload @{ error = 'No local fallback radar image available' }
        return
      }

      try {
        $bytes = [System.IO.File]::ReadAllBytes($RadarFallbackImagePath)
        Send-Response -Stream $Stream -StatusCode 200 -Body $bytes -ContentType 'image/png' -CacheControl 'public, max-age=60'
      } catch {
        Send-Json -Stream $Stream -StatusCode 500 -Payload @{
          error = 'Failed to read local fallback radar image'
          detail = $_.Exception.Message
        }
      }
      return
    }
    '/api/lightning' {
      try {
        $payload = Get-LightningPayload -Query $request.Query
        Send-Json -Stream $Stream -StatusCode 200 -Payload $payload
      } catch {
        Send-Json -Stream $Stream -StatusCode 502 -Payload @{
          error = 'Failed to fetch FreeLightning feed'
          detail = $_.Exception.Message
        }
      }
      return
    }
    default {
      try {
        $file = Get-StaticFile -RequestPath $request.Path
        Send-Response -Stream $Stream -StatusCode 200 -Body $file.Bytes -ContentType $file.ContentType -CacheControl 'no-cache'
      } catch [System.UnauthorizedAccessException] {
        Send-Json -Stream $Stream -StatusCode 403 -Payload @{ error = 'Forbidden' }
      } catch [System.IO.FileNotFoundException] {
        Send-Json -Stream $Stream -StatusCode 404 -Payload @{ error = 'Not found' }
      } catch {
        Send-Json -Stream $Stream -StatusCode 500 -Payload @{
          error = 'Failed to serve static asset'
          detail = $_.Exception.Message
        }
      }
      return
    }
  }
}

$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse($HostName), $Port)
$listener.Server.ReceiveTimeout = 500
$listener.Server.SendTimeout = 500
$listener.Start()
Load-LightningHistory
Initialize-RadarFrames

Write-Host "IMS radar viewer running at http://$HostName`:$Port"

try {
  while ($true) {
    if (-not $listener.Pending()) {
      Start-Sleep -Milliseconds 200
      continue
    }

    $client = $listener.AcceptTcpClient()

    try {
      $stream = $client.GetStream()
      $reader = [System.IO.StreamReader]::new($stream, [System.Text.Encoding]::ASCII, $false, 4096, $true)

      $requestLine = $reader.ReadLine()
      if (-not $requestLine) {
        $client.Close()
        continue
      }

      while ($true) {
        $line = $reader.ReadLine()
        if ([string]::IsNullOrEmpty($line)) {
          break
        }
      }

      $parts = $requestLine.Split(' ')
      if ($parts.Count -lt 2) {
        Send-Json -Stream $stream -StatusCode 400 -Payload @{ error = 'Malformed request line' }
      } else {
        Handle-Request -Method $parts[0] -Target $parts[1] -Stream $stream
      }
    } catch {
      try {
        if ($stream) {
          Send-Json -Stream $stream -StatusCode 500 -Payload @{
            error = 'Unhandled server error'
            detail = $_.Exception.Message
          }
        }
      } catch {
      }
    } finally {
      if ($reader) {
        $reader.Dispose()
      }

      if ($stream) {
        $stream.Dispose()
      }

      $client.Close()
    }
  }
} finally {
  $listener.Stop()
}
