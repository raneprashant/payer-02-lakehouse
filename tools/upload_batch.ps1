<#
Upload one generated batch ONLY into the intended catalog landing volume.
Run from payer-02-lakehouse project root using Windows PowerShell:
  .\tools\upload_batch.ps1 -GeneratorRoot "C:\Projects\payer-02-data-generator" -Scale MEDIUM -Batch B0001
Avoid uploading B0002..B0007 before the corresponding pipeline lesson.
#>
param(
  [Parameter(Mandatory=$true)][string]$GeneratorRoot,
  [ValidateSet('SMOKE','MEDIUM','LARGE')][string]$Scale='MEDIUM',
  [Parameter(Mandatory=$true)][ValidatePattern('^B\d{4}$')][string]$Batch,
  [ValidateSet('payer_02_dev','payer_02_test','payer_02_prod')][string]$Catalog='payer_02_dev',
  [string]$Profile='DATABRICKS_FREE'
)
$ErrorActionPreference='Stop'
$source = Join-Path $GeneratorRoot "output/$Scale/landing"
if (-not (Test-Path $source)) { throw "Generator landing folder not found: $source" }
$matches = @(Get-ChildItem $source -Recurse -File | Where-Object { $_.Name -match "_$Batch`_" })
if ($matches.Count -eq 0) { throw "No files for $Batch under $source" }
$remote = "dbfs:/Volumes/$Catalog/landing/raw_files/landing"
Write-Host "Uploading $($matches.Count) files for $Batch to $Catalog (no overwrite)"
foreach ($f in $matches) {
  $entity = $f.Directory.Name
  $destDir = "$remote/$entity"
  databricks fs mkdirs $destDir --profile $Profile
  if ($LASTEXITCODE -ne 0) { throw "Failed to create $destDir" }
  databricks fs cp $f.FullName "$destDir/$($f.Name)" --profile $Profile
  if ($LASTEXITCODE -ne 0) { throw "Upload failed for $($f.FullName)" }
}
Write-Host "Uploaded $($matches.Count) files. Verify with: databricks fs ls $remote/member_master --profile $Profile"
