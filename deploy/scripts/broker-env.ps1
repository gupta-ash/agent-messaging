# Shared broker.env grammar for Windows operators and Codex URL selection.
# Trim lines; skip blanks/comments; uppercase keys allow spaces/tabs around '='.
# Trim values, remove matching outer quotes, then trim the quoted value again.
# Every port declaration must be canonical decimal 1..65535, including duplicates.
# Port-only readers validate the same file without importing or retaining secrets.
function Read-BrokerEnvFile {
    param([string]$Path, [switch]$PortOnly, [switch]$AllowMissing)

    $values = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        if ($AllowMissing) { return $values }
        throw "broker.env not found at $Path. Run the exact-version installer first."
    }
    $reader = [IO.StreamReader]::new($Path)
    try {
        while ($null -ne ($raw = $reader.ReadLine())) {
            $line = $raw.Trim()
            if ($line -eq '' -or $line.StartsWith('#')) { continue }
            if ($line -cnotmatch '^([A-Z_][A-Z0-9_]*)[ \t]*=[ \t]*(.*)$') {
                if ($line.IndexOf('BROKER_PORT', [StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    throw 'broker.env has invalid BROKER_PORT declaration.'
                }
                continue
            }
            $key = $matches[1]
            $value = $matches[2].Trim()
            $quote = if ($value.Length -gt 0) { $value.Substring(0, 1) } else { '' }
            if ($quote -eq '"' -or $quote -eq "'") {
                if ($value.Length -lt 2 -or -not $value.EndsWith($quote, [StringComparison]::Ordinal)) {
                    if ($key -eq 'BROKER_PORT') { throw 'broker.env has invalid BROKER_PORT (mismatched quotes).' }
                    throw 'Mismatched quotes in private broker.env.'
                }
                $value = $value.Substring(1, $value.Length - 2).Trim()
            } elseif ($value.EndsWith('"', [StringComparison]::Ordinal) -or $value.EndsWith("'", [StringComparison]::Ordinal)) {
                if ($key -eq 'BROKER_PORT') { throw 'broker.env has invalid BROKER_PORT (mismatched quotes).' }
                throw 'Mismatched quotes in private broker.env.'
            }
            if ($key -eq 'BROKER_PORT' -and ($value -cnotmatch '^[1-9][0-9]{0,4}$' -or [int]$value -gt 65535)) {
                throw 'broker.env has invalid BROKER_PORT; expected canonical decimal 1 through 65535.'
            }
            if (-not $PortOnly -or $key -eq 'BROKER_PORT') { $values[$key] = $value }
        }
    } finally { $reader.Dispose() }
    return $values
}

function Get-BrokerEnvPort {
    param([string]$Path)

    $values = Read-BrokerEnvFile -Path $Path -PortOnly -AllowMissing
    if ($values.ContainsKey('BROKER_PORT')) { return [int]$values['BROKER_PORT'] }
    return 8787
}
