# Settings
$sourcePath = "C:\Media\Zips"
$targetPath = "S:\TestBackup"
$chunkSize = 1GB
$manifestFile = Join-Path $targetPath "manifest.json"

# Create target folder if it doesn't exist
if (-not (Test-Path $targetPath)) {
    New-Item -ItemType Directory -Path $targetPath -Force | Out-Null
}

# Load existing manifest
$manifest = @{}
if (Test-Path $manifestFile) {
    $json = Get-Content $manifestFile -Raw | ConvertFrom-Json
    foreach ($key in $json.PSObject.Properties.Name) {
        $manifest[$key] = @{}
        foreach ($subkey in $json.$key.PSObject.Properties.Name) {
            $manifest[$key][$subkey] = $json.$key.$subkey
        }
    }
}

# Hash function
function Get-Hash([byte[]]$data) {
    if (-not $data -or $data.Length -eq 0) {
        throw "Get-Hash received null or empty data!"
    }
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        # Explicit cast to byte[] to resolve ambiguity
        $hashBytes = $sha256.ComputeHash([byte[]]$data)
        return ([System.BitConverter]::ToString($hashBytes)).Replace("-", "").ToLower()
    }
    finally {
        $sha256.Dispose()
    }
}

# Counters
$totalChunks = 0
$updatedChunks = 0
$newChunks = 0
$untouchedChunks = 0
$outdatedChunks = 0
$processedFiles = @{}

# Process files
Get-ChildItem -Path $sourcePath -Recurse -File | ForEach-Object {
    $file = $_
    $filePath = $file.FullName
    $fileName = $file.Name
    $processedFiles[$fileName] = $true

    Write-Host "Processing file: $filePath"

    if (-not $manifest.ContainsKey($fileName)) {
        $manifest[$fileName] = @{}
    }

    $stream = [System.IO.File]::OpenRead($filePath)
    try {
        $chunkIndex = 0
        $buffer = New-Object byte[] $chunkSize

        while (($bytesRead = $stream.Read($buffer, 0, $chunkSize)) -gt 0) {
            # Defensive check (should not be needed, but just in case)
            if ($bytesRead -le 0) { break }

            # Copy only the bytes read into a new array
            $chunkData = New-Object byte[] $bytesRead
            [Array]::Copy($buffer, 0, $chunkData, 0, $bytesRead)

            $chunkHash = Get-Hash $chunkData
            $chunkName = "chunk$chunkIndex"

            $safeFileName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
            $fileExtension = [System.IO.Path]::GetExtension($fileName)
            $chunkFileName = "$safeFileName-$chunkName$($fileExtension).bin"
            $chunkFilePath = Join-Path $targetPath $chunkFileName

            if (-not $manifest[$fileName].ContainsKey($chunkName)) {
                Write-Host "  Creating new chunk #$chunkIndex (Hash: $chunkHash)"
                $manifest[$fileName][$chunkName] = $chunkHash
                [System.IO.File]::WriteAllBytes($chunkFilePath, $chunkData)
                $newChunks++
            } elseif ($manifest[$fileName][$chunkName] -ne $chunkHash) {
                Write-Host "  Updating chunk #$chunkIndex (Hash: $chunkHash)"
                $manifest[$fileName][$chunkName] = $chunkHash
                [System.IO.File]::WriteAllBytes($chunkFilePath, $chunkData)
                $updatedChunks++
            } else {
                Write-Host "  Chunk #$chunkIndex unchanged, skipping."
                $untouchedChunks++
            }

            $totalChunks++
            $chunkIndex++
        }
    }
    finally {
        $stream.Close()
        $stream.Dispose()
    }
}

# Clean manifest entries for files no longer present
$manifest.Keys | Where-Object { -not $processedFiles.ContainsKey($_) } | ForEach-Object {
    Write-Host "Removing obsolete manifest entry: $_"
    $manifest.Remove($_)
}

# Track all valid chunk files
$validChunkFiles = @{}
foreach ($fileName in $manifest.Keys) {
    $safeFileName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
    $fileExtension = [System.IO.Path]::GetExtension($fileName)
    foreach ($chunkName in $manifest[$fileName].Keys) {
        $chunkFileName = "$safeFileName-$chunkName$($fileExtension).bin"
        $validChunkFiles[$chunkFileName] = $true
    }
}

# Delete orphaned chunks
Get-ChildItem -Path $targetPath -Filter *.bin | ForEach-Object {
    if (-not $validChunkFiles.ContainsKey($_.Name)) {
        Write-Host "  Removing outdated chunk: $($_.FullName)"
        Remove-Item $_.FullName -Force
        $outdatedChunks++
    }
}

# Save manifest
$manifest | ConvertTo-Json -Depth 10 | Set-Content $manifestFile

# Summary
Write-Host "`nChunking complete."
Write-Host "Total chunks processed: $totalChunks"
Write-Host "New chunks created: $newChunks"
Write-Host "Chunks updated: $updatedChunks"
Write-Host "Chunks untouched: $untouchedChunks"
Write-Host "Chunks removed: $outdatedChunks"
