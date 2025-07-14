# === Settings ===
$sourcePath = "S:\Source"
$targetPath = "T:\Backup"
$manifestFile = Join-Path $targetPath "manifest.json"

$avgChunkSize = 256MB
$minChunkSize = 64KB
$maxChunkSize = 512MB

$rabinWindowSize = 48
$rabinPolynomial = 8286094103145083 # Example 53-bit irreducible poly

$cpuThreads = [Environment]::ProcessorCount
$throttleLimit = $cpuThreads

# === Load Manifest ===
if (Test-Path $manifestFile) {
    $manifest = ConvertFrom-Json (Get-Content $manifestFile -Raw) -AsHashtable
} else {
    $manifest = @{}
}

# === Utility Functions ===

function Ensure-TargetDirectory {
    param([string]$path)
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Path $path -Force | Out-Null
    }
}

function Save-Manifest {
    param([hashtable]$manifest, [string]$path)
    $manifest | ConvertTo-Json -Depth 10 | Set-Content $path
}

function Cleanup-OrphanChunks {
    param([hashtable]$manifest, [string]$targetPath)
    $validChunks = @{}
    foreach ($entry in $manifest.Values) {
        foreach ($chunkHash in $entry.ChunkHashes) {
            $validChunks["$chunkHash.bin"] = $true
        }
    }
    Get-ChildItem -Path $targetPath -Filter "*.bin" | ForEach-Object {
        if (-not $validChunks.ContainsKey($_.Name)) {
            Remove-Item $_.FullName -Force
            Write-Host "Removed orphaned chunk: $($_.Name)"
        }
    }
}

function Get-RabinChunkBoundaries {
    param(
        [byte[]]$data,
        [int64]$poly,
        [int]$avgSize,
        [int]$minSize,
        [int]$maxSize,
        [int]$windowSize
    )

    # Simplified Rabin fingerprint simulation for demo purposes
    $modulus = [math]::Pow(2,53) - 1  # approximate 53-bit modulus
    $mask = $avgSize - 1
    $hash = 0
    $chunks = @()
    $start = 0
    $length = $data.Length

    for ($i = 0; $i -lt $length; $i++) {
        $hash = (($hash -shl 1) + $data[$i]) % $modulus
        $chunkLen = $i - $start + 1

        if (
            ($chunkLen -ge $minSize -and ($hash -band $mask) -eq 0) -or
            $chunkLen -ge $maxSize
        ) {
            $chunks += ,@($start, $chunkLen)
            $start = $i + 1
        }
    }

    if ($start -lt $length) {
        $finalLen = $length - $start
        $chunks += ,@($start, $finalLen)
    }

    return $chunks
}

function Compute-Sha256Hash {
    param([byte[]]$data)
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha256.ComputeHash($data)
        return ([BitConverter]::ToString($hash) -replace "-", "").ToLower()
    } finally {
        $sha256.Dispose()
    }
}

# === Start ===
$globalStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
Write-Host "Starting content-defined backup with Rabin Fingerprinting..."
Write-Host "Detected $cpuThreads logical threads"

Ensure-TargetDirectory -Path $targetPath

$counters = @{ New = 0; Unchanged = 0; TotalBytesWritten = 0 }
$totalChunks = 0
$totalDataBytes = 0

$files = Get-ChildItem -Path $sourcePath -File -Recurse

# Constants for reading progress
$progressIntervalSec = 3

foreach ($file in $files) {
    Write-Host "Processing: $($file.FullName)"
    $fileStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    $stream = $file.OpenRead()
    $reader = New-Object System.IO.BinaryReader($stream)

    $chunks = @()
    $offset = 0
    $fileLength = $stream.Length
    $lastProgressUpdate = [DateTime]::UtcNow

    $bufferSize = 1MB  # Smaller buffer for more frequent progress updates

    try {
        while ($offset -lt $fileLength) {
            $remaining = $fileLength - $offset
            $toRead = if ($remaining -gt $bufferSize) { $bufferSize } else { $remaining }

            $data = $reader.ReadBytes([int]$toRead)
            if ($data.Length -eq 0) { break }

            # Get chunk boundaries within this data block
            $boundaries = Get-RabinChunkBoundaries -data $data -poly $rabinPolynomial `
                -avgSize $avgChunkSize -minSize $minChunkSize -maxSize $maxChunkSize -windowSize $rabinWindowSize

            foreach ($b in $boundaries) {
                $start = $b[0]
                $length = $b[1]
                if ($start + $length -gt $data.Length) { continue }
                $chunkData = $data[$start..($start + $length - 1)]
                $chunks += [PSCustomObject]@{
                    Offset = $offset + $start
                    Data   = $chunkData
                }
            }

            $offset += $data.Length

            # Update reading progress every $progressIntervalSec seconds or on last chunk
            $now = [DateTime]::UtcNow
            if (($now - $lastProgressUpdate).TotalSeconds -ge $progressIntervalSec -or $offset -ge $fileLength) {
                $percent = [math]::Round(($offset / $fileLength) * 100, 2)
                $remainingPercent = 100 - $percent

                $elapsed = $fileStopwatch.Elapsed.TotalSeconds
                $etaSeconds = if ($offset -gt 0) {
                    $elapsed * ($fileLength - $offset) / $offset
                } else { 0 }
                $etaTimeSpan = [TimeSpan]::FromSeconds($etaSeconds)
                $etaString = '{0:D2}:{1:D2}:{2:D2}' -f $etaTimeSpan.Hours, $etaTimeSpan.Minutes, $etaTimeSpan.Seconds

                Write-Host -NoNewline ("`rReading {0} [Progress: {1}% | Remaining: {2}% | ETA: {3}]    " -f $file.Name, $percent, $remainingPercent, $etaString)
                [Console]::Out.Flush()
                $lastProgressUpdate = $now
            }
        }
    }
    finally {
        $reader.Close()
        $stream.Close()
    }

    # End progress line with newline
    Write-Host ""

    $totalChunks += $chunks.Count
    $totalDataBytes += ($chunks | ForEach-Object { $_.Data.Length } | Measure-Object -Sum).Sum

    # === Hash all chunks in parallel ===
    $results = $chunks | ForEach-Object -Parallel {
        function Compute-Sha256Hash {
            param([byte[]]$data)
            $sha256 = [System.Security.Cryptography.SHA256]::Create()
            try {
                $hash = $sha256.ComputeHash($data)
                return ([BitConverter]::ToString($hash) -replace "-", "").ToLower()
            } finally {
                $sha256.Dispose()
            }
        }
        $hash = Compute-Sha256Hash -data $_.Data
        [PSCustomObject]@{
            Offset = $_.Offset
            Data   = $_.Data
            Hash   = $hash
        }
    } -ThrottleLimit $throttleLimit

    # Ensure manifest entry exists
    if (-not $manifest.ContainsKey($file.FullName)) {
        $manifest[$file.FullName] = @{ ChunkHashes = @() }
    }

    # === Write chunks and update manifest sequentially ===
    foreach ($chunk in $results) {
        $hash = $chunk.Hash
        $chunkFile = Join-Path $targetPath "$hash.bin"

        if (-not $manifest[$file.FullName].ChunkHashes.Contains($hash)) {
            if (-not (Test-Path $chunkFile)) {
                [System.IO.File]::WriteAllBytes($chunkFile, $chunk.Data)
                $counters["New"]++
                $counters["TotalBytesWritten"] += $chunk.Data.Length
                Write-Host "  New chunk: $hash"
            } else {
                Write-Host "  Chunk already exists: $hash"
            }
            $manifest[$file.FullName].ChunkHashes += $hash
        } else {
            $counters["Unchanged"]++
        }
    }

    $fileStopwatch.Stop()
    Write-Host ("Finished {0} in {1:N2} sec ({2:N2} MB)" -f `
        $file.Name, $fileStopwatch.Elapsed.TotalSeconds, ($file.Length / 1MB))
}

Cleanup-OrphanChunks -manifest $manifest -targetPath $targetPath
Save-Manifest -manifest $manifest -Path $manifestFile

$globalStopwatch.Stop()

# === Metrics Summary ===
$duration = $globalStopwatch.Elapsed.TotalSeconds
$totalMB = $counters["TotalBytesWritten"] / 1MB
$speed = if ($duration -gt 0) { $totalMB / $duration } else { 0 }

Write-Host "`nBackup complete."
Write-Host "Chunks new: $($counters.New), unchanged: $($counters.Unchanged)"
Write-Host ("Total MB written: {0:N2}" -f $totalMB)
Write-Host ("Total time: {0:N2} seconds" -f $duration)
Write-Host ("Average speed: {0:N2} MB/sec" -f $speed)
