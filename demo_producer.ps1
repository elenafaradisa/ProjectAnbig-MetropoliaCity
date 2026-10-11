# Demo helper: continue the Kafka replay right after the last event that is
# already in core.observations_stream, so a demo never re-sends old data.
#
# Usage (from the project root, Kafka + streaming job already running):
#   .\demo_producer.ps1              # 600 events, 1 per second (~10 minutes)
#   .\demo_producer.ps1 -Limit 120   # shorter demo
#   .\demo_producer.ps1 -Interval 0.5
#   .\demo_producer.ps1 -Limit 1000 -BurstMax 80            # burst: 1-80 random events per second
#   .\demo_producer.ps1 -Limit 1000 -BurstMin 10 -BurstMax 80
param(
    [int]$Limit = 600,
    [double]$Interval = 1.0,
    [int]$BurstMin = 1,
    [int]$BurstMax = 0      # > 0 switches producer.py to burst mode
)

# Invariant culture: on an Indonesian Windows locale 0.5 would become "0,5",
# which producer.py's float() argument cannot parse.
$IntervalArg = $Interval.ToString([System.Globalization.CultureInfo]::InvariantCulture)

$Pacing = if ($BurstMax -gt 0) { @("--burst-min", $BurstMin, "--burst-max", $BurstMax) } else { @("--interval", $IntervalArg) }
$PacingText = if ($BurstMax -gt 0) { "$BurstMin-$BurstMax event acak per detik" } else { "jeda $Interval detik" }

# Last event time already stored (event time, not wall-clock time). Empty
# table -> start from the beginning of the dataset.
$last = docker compose exec -T postgres psql -U metropolia -d metropolia -tA `
    -c "SELECT COALESCE(to_char(max(observed_at) + interval '1 second', 'YYYY-MM-DD HH24:MI:SS'), '') FROM core.observations_stream"
$last = ($last | Out-String).Trim()

if ($LASTEXITCODE -ne 0) {
    Write-Host "Tidak bisa membaca core.observations_stream. Pastikan container postgres berjalan." -ForegroundColor Red
    exit 1
}

if ($last -eq "") {
    Write-Host "Tabel streaming masih kosong: mulai dari awal dataset."
    docker compose run --rm producer python producer.py --topic traffic_topic `
        --bootstrap-servers kafka:9092 @Pacing --limit $Limit
} else {
    Write-Host "Event terakhir di database sebelum $last. Melanjutkan dari situ ($Limit event, $PacingText)."
    docker compose run --rm producer python producer.py --topic traffic_topic `
        --bootstrap-servers kafka:9092 @Pacing --limit $Limit --start "$last"
}
