# === Settings ===
$sourcePath = "S:\Source"          # Change to your source directory
$targetPath = "T:\Backup"          # Change to your backup directory
$manifestFile = Join-Path $targetPath "manifest.json"

$chunkSizeMB = 256                 # Chunk size in MB (adjust as needed)
$chunkSize = $chunkSizeMB * 1MB

# Get number of logical CPU threads
$cpuThreads = [Environment]::ProcessorCount

# Set batch size and throttle limit to CPU thread count
$batchSize = $cpuThreads
$throttleLimit = $cpuThreads

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
    [System.IO.File]::WriteAllBytes($chunkFilePath, $chunkData)
    $writeStopwatch.Stop()

    $performance.WriteTimes += $writeStopwatch.Elapsed.TotalSeconds
    $performance.WriteSizes += $chunkData.Length

    $percent = [math]::Round(($bytesWrittenSoFar / $totalFileSize) * 100, 2)
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
    }
    else {
        0
    }

    $chunksRemaining = $totalChunks - ($chunkIndex + 1)
    Write-Host ("    Chunk written: {0}, Size: {1} bytes, Time: {2:N3} s, Speed: {3:N2} MB/s, Chunks remaining: {4}" -f `
         $chunkName, $chunkData.Length, $writeStopwatch.Elapsed.TotalSeconds, $writeSpeedMBps, $chunksRemaining)
    Write-Host ("    Progress: {0}%   ETA: {1}" -f $percent, $etaFormatted)
}

# === Main Script ===

Write-Host "Starting backup..."
Write-Host "Using $cpuThreads CPU threads for batch size and parallelism."

Ensure-TargetDirectory -Path $targetPath

if (Test-Path $manifestFile) {
    $manifest = ConvertFrom-Json (Get-Content $manifestFile -Raw) -AsHashtable
}
else {
    $manifest = @{}
}

$counters = @{ New = 0; Updated = 0; Unchanged = 0; Outdated = 0 }

$files = Get-ChildItem -Path $sourcePath -File -Recurse

foreach ($file in $files) {
    Write-Host "Processing file: $($file.FullName)"
    $stream = [System.IO.File]::OpenRead($file.FullName)
    $chunkIndex = 0
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $totalChunks = [math]::Ceiling($file.Length / $chunkSize)
    $bytesWrittenSoFar = 0

    try {
        $chunksBatch = @()

        while ($true) {
            # Calculate how many bytes to read (last chunk may be smaller)
            $remainingBytes = $file.Length - ($chunkIndex * $chunkSize)
            if ($remainingBytes -le 0) { break }
            $bytesToRead = if ($remainingBytes -gt $chunkSize) { $chunkSize } else { [int]$remainingBytes }

            $buffer = New-Object byte[] $bytesToRead
            $bytesRead = $stream.Read($buffer, 0, $bytesToRead)
            if ($bytesRead -le 0) { break }

            # Store chunk info for batch processing
            $chunksBatch += [pscustomobject]@{
                Index = $chunkIndex
                Data  = $buffer
            }

            # When batch full or last chunk, process batch
            if ($chunksBatch.Count -ge $batchSize -or $bytesRead -lt $chunkSize) {

                # Parallel hash computation
                $hashResults = $chunksBatch | ForEach-Object -Parallel {
                    $sha256 = [System.Security.Cryptography.SHA256]::Create()
                    try {
                        $hashBytes = $sha256.ComputeHash($_.Data)
                    }
                    finally {
                        $sha256.Dispose()
                    }
                    [pscustomobject]@{
                        Index = $_.Index
                        Hash  = ([System.BitConverter]::ToString($hashBytes)).Replace("-", "").ToLower()
                        Data  = $_.Data
                    }
                } -ThrottleLimit $throttleLimit

                foreach ($chunk in $hashResults) {
                    $chunkName = $chunk.Index.ToString()
                    if (-not $manifest.ContainsKey($file.Name)) {
                        $manifest[$file.Name] = @{}
                    }
                    $existingHash = if ($manifest[$file.Name].ContainsKey($chunkName)) { $manifest[$file.Name][$chunkName] } else { "" }

                    if ($existingHash -ne $chunk.Hash) {
                        $bytesWrittenSoFar += $chunk.Data.Length
                        Write-ChunkFile -targetDir $targetPath -FileName $file.Name -chunkName $chunkName -chunkData $chunk.Data `
                           -totalFileSize $file.Length -bytesWrittenSoFar $bytesWrittenSoFar -stopwatch ([ref]$stopwatch) `
                           -chunkIndex $chunk.Index -totalChunks $totalChunks

                        if ($existingHash -eq "") {
                            $counters["New"]++
                            Write-Host "  New chunk $chunkName"
                        }
                        else {
                            $counters["Updated"]++
                            Write-Host "  Updated chunk $chunkName"
                        }
                        $manifest[$file.Name][$chunkName] = $chunk.Hash
                        Save-Manifest -manifest $manifest -Path $manifestFile
                    }
                    else {
                        $counters["Unchanged"]++
                        $bytesWrittenSoFar += $chunk.Data.Length
                        $percent = [math]::Round(($bytesWrittenSoFar / $file.Length) * 100, 2)
                        $elapsed = $stopwatch.Elapsed.TotalSeconds
                        $etaFormatted = "Calculating..."
                        if ($bytesWrittenSoFar -gt 0) {
                            $rate = $bytesWrittenSoFar / $elapsed
                            $remaining = $file.Length - $bytesWrittenSoFar
                            $etaSeconds = $remaining / $rate
                            $etaFormatted = [timespan]::FromSeconds($etaSeconds).ToString("hh\:mm\:ss")
                        }
                        Write-Host ("    Progress: {0}%   ETA: {1}" -f $percent, $etaFormatted)
                    }
                }

                $chunksBatch = @() # reset batch
            }

            $chunkIndex++
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
