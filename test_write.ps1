# === Settings ===
$sourcePath = "S:\Source" # Change to your source directory
$targetPath = "T:\Backup" # Change to your backup directory
$manifestFile = Join-Path $targetPath "manifest.json"

$chunkSizeMB = 256 # Chunk size in MB (adjust as needed)
$chunkSize = $chunkSizeMB * 1MB
$batchSize = 16 # Number of chunks to process per batch

# === Performance tracking ===
$performance = @{
  ReadTimes = @()
  ReadSizes = @()
  WriteTimes = @()
  WriteSizes = @()
}

# === Helper Functions ===

function Ensure-TargetDirectory {
  param([string]$path)
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function Cleanup-OrphanChunks {
  param(
    [hashtable]$manifest,
    [string]$targetPath,
    [ref]$counters
  )
  $validChunks = @{}
  foreach ($fileName in $manifest.Keys) {
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
    $extension = [System.IO.Path]::GetExtension($fileName)
    foreach ($chunkName in $manifest[$fileName].Keys) {
      $chunkFileName = "$baseName-$chunkName$extension.bin"
      $validChunks[$chunkFileName] = $true
    }
  }

  Get-ChildItem -Path $targetPath -Filter *.bin | ForEach-Object {
    if (-not $validChunks.ContainsKey($_.Name)) {
      Write-Host "Removing outdated chunk: $($_.FullName)"
      Remove-Item $_.FullName -Force
      $counters.Value["Outdated"]++
    }
  }
}

function Save-Manifest {
  param(
    [hashtable]$manifest,
    [string]$path
  )
  $manifest | ConvertTo-Json -Depth 10 | Set-Content $path
}

function Write-ChunkFile {
  param(
    [string]$targetDir,
    [string]$fileName,
    [string]$chunkName,
    [byte[]]$chunkData,
    [int64]$totalFileSize,
    [int64]$bytesWrittenSoFar,
    [ref]$stopwatch,
    [int]$chunkIndex,
    [int]$totalChunks
  )
  $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
  $extension = [System.IO.Path]::GetExtension($fileName)
  $chunkFileName = "$baseName-$chunkName$extension.bin"
  $chunkFilePath = Join-Path $targetDir $chunkFileName

  $writeStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
  [System.IO.File]::WriteAllBytes($chunkFilePath,$chunkData)
  $writeStopwatch.Stop()

  $performance.WriteTimes += $writeStopwatch.Elapsed.TotalSeconds
  $performance.WriteSizes += $chunkData.Length

  $percent = [math]::Round(($bytesWrittenSoFar / $totalFileSize) * 100,2)
  $elapsed = $stopwatch.Value.Elapsed.TotalSeconds
  $etaFormatted = "Calculating..."
  if ($bytesWrittenSoFar -gt 0) {
    $rate = $bytesWrittenSoFar / $elapsed
    $remaining = $totalFileSize - $bytesWrittenSoFar
    $etaSeconds = $remaining / $rate
    $etaFormatted = [timespan]::FromSeconds($etaSeconds).ToString("hh\:mm\:ss")
  }

  $writeSpeedMBps = if ($writeStopwatch.Elapsed.TotalSeconds -gt 0) {
    $chunkData.Length / $writeStopwatch.Elapsed.TotalSeconds / 1MB
  } else { 0 }

  $chunksRemaining = $totalChunks - ($chunkIndex + 1)
  Write-Host ("    Chunk written: {0}, Size: {1} bytes, Time: {2:N3} s, Speed: {3:N2} MB/s, Chunks remaining: {4}" -f `
       $chunkName,$chunkData.Length,$writeStopwatch.Elapsed.TotalSeconds,$writeSpeedMBps,$chunksRemaining)
  Write-Host ("    Progress: {0}%   ETA: {1}" -f $percent,$etaFormatted)
}

# === Main Script ===

Write-Host "Starting backup..."

Ensure-TargetDirectory -Path $targetPath

if (Test-Path $manifestFile) {
  $manifest = ConvertFrom-Json (Get-Content $manifestFile -Raw) -AsHashTable
} else {
  $manifest = @{}
}

$counters = @{ new = 0; Updated = 0; Unchanged = 0; Outdated = 0 }

$files = Get-ChildItem -Path $sourcePath -File -Recurse

foreach ($file in $files) {
  Write-Host "Processing file: $($file.FullName)"
  $stream = [System.IO.File]::OpenRead($file.FullName)
  $chunkIndex = 0
  $buffer = New-Object byte[] $chunkSize
  $bytesWrittenSoFar = 0
  $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
  $totalChunks = [math]::Ceiling($file.Length / $chunkSize)

  try {
    while ($true) {
      $chunkBatches = @()

      for ($i = 0; $i -lt $batchSize; $i++) {
        $bytesRead = $stream.Read($buffer,0,$chunkSize)
        if ($bytesRead -eq 0) {
          break
        }

        $readStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        $chunkData = New-Object byte[] $bytesRead
        [System.Buffer]::BlockCopy($buffer,0,$chunkData,0,$bytesRead)
        $readStopwatch.Stop()

        $performance.ReadTimes += $readStopwatch.Elapsed.TotalSeconds
        $performance.ReadSizes += $bytesRead

        $chunkBatches += [pscustomobject]@{
          Index = $chunkIndex
          Data = $chunkData
          Size = $bytesRead
        }

        $chunkIndex++
      }

      if ($chunkBatches.Count -eq 0) {
        break
      }

      foreach ($chunk in $chunkBatches) {
        $sha256 = [System.Security.Cryptography.SHA256]::Create()
        try {
          $hashBytes = $sha256.ComputeHash($chunk.Data)
        } finally {
          $sha256.Dispose()
        }
        $chunkHash = ([System.BitConverter]::ToString($hashBytes)).Replace("-","").ToLower()
        $chunkName = $chunk.Index.ToString()

        if (-not $manifest.ContainsKey($file.Name)) {
          $manifest[$file.Name] = @{}
        }

        $existingHash = if ($manifest[$file.Name].ContainsKey($chunkName)) {
          $manifest[$file.Name][$chunkName]
        } else { "" }

        if ($existingHash -ne $chunkHash) {
          $bytesWrittenSoFar += $chunk.Size
          Write-ChunkFile -targetDir $targetPath -FileName $file.Name -chunkName $chunkName -chunkData $chunk.Data `
             -totalFileSize $file.Length -bytesWrittenSoFar $bytesWrittenSoFar -stopwatch ([ref]$stopwatch) `
             -chunkIndex $chunk.Index -totalChunks $totalChunks

          if ($existingHash -eq "") {
            $counters["New"]++
            Write-Host "  New chunk $chunkName"
          } else {
            $counters["Updated"]++
            Write-Host "  Updated chunk $chunkName"
          }

          $manifest[$file.Name][$chunkName] = $chunkHash
          Save-Manifest -manifest $manifest -Path $manifestFile
        } else {
          $counters["Unchanged"]++
          $bytesWrittenSoFar += $chunk.Size
          $percent = [math]::Round(($bytesWrittenSoFar / $file.Length) * 100,2)
          $elapsed = $stopwatch.Elapsed.TotalSeconds
          $etaFormatted = "Calculating..."
          if ($bytesWrittenSoFar -gt 0) {
            $rate = $bytesWrittenSoFar / $elapsed
            $remaining = $file.Length - $bytesWrittenSoFar
            $etaSeconds = $remaining / $rate
            $etaFormatted = [timespan]::FromSeconds($etaSeconds).ToString("hh\:mm\:ss")
          }
          Write-Host ("    Progress: {0}%   ETA: {1}" -f $percent,$etaFormatted)
        }
      }
    }
  }
  finally {
    $stream.Dispose()
  }
}

Cleanup-OrphanChunks -manifest $manifest -targetPath $targetPath -counters ([ref]$counters)
Save-Manifest -manifest $manifest -Path $manifestFile

Write-Host "Backup complete."
Write-Host "Chunks new: $($counters.New), updated: $($counters.Updated), unchanged: $($counters.Unchanged), removed outdated: $($counters.Outdated)"
