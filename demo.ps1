# One-command streaming demo.
#
#   .\demo.ps1                              # send 600 events, 1 per second
#   .\demo.ps1 -Limit 1000 -BurstMax 50     # send 1000 events, 1-50 per second (random)
#   .\demo.ps1 -Check                       # pre-demo check; fixes what it can, sends nothing
#   .\demo.ps1 -Reset                       # empty the streaming tables, start the replay from 2022
#   .\demo.ps1 -Stop                        # stop the streaming job
#
# (If Windows blocks the script: powershell -ExecutionPolicy Bypass -File .\demo.ps1 ...)
#
# The streaming job runs ONLY in the stream-consumer service (docker-compose.yml,
# restart: unless-stopped). One container means never two jobs at once, and
# Docker restarts the job on its own if it dies; it resumes from its checkpoint.
# This script only:
#   1. makes sure stream-consumer runs and its queries have started
#      (and removes old jobs left in airflow-scheduler by earlier manual runs);
#   2. sends events with the producer, continuing right after the last event
#      already in core.observations_stream -- existing data is kept.
# -Reset empties core.observations_stream, mart.road_hourly_stream and the
# checkpoints. The batch tables are never touched.
param(
    [int]$Limit = 600,
    [double]$Interval = 1.0,
    [int]$BurstMin = 1,
    [int]$BurstMax = 0,
    [switch]$Reset,
    [switch]$Stop,
    [switch]$Check
)

# Continue, not Stop: Windows PowerShell 5 turns progress text that docker
# writes to stderr into errors. Exit codes are checked by hand instead.
$ErrorActionPreference = "Continue"
$DatasetEnd = "2024-01-01 00:00:00"   # last timestamp in traffictab23
$Consumer = "stream-consumer"
$ReadyText = "Streaming queries started"
# Old jobs started by hand inside airflow-scheduler (before stream-consumer
# existed). Found through /proc because the image has no ps.
$FindGhosts = "grep -l -a -P '\x00-m\x00[s]park\.jobs\.stream_transform' /proc/[0-9]*/cmdline 2>/dev/null | cut -d/ -f3"

function Step($text) { Write-Host "`n== $text" -ForegroundColor Cyan }
function Ok($text)   { Write-Host "  [ OK ]  $text" -ForegroundColor Green }
function Bad($text)  { Write-Host "  [GAGAL] $text" -ForegroundColor Red }
function Note($text) { Write-Host "  [INFO]  $text" -ForegroundColor Yellow }

function Psql($sql) {
    $out = docker compose exec -T postgres psql -U metropolia -d metropolia -tA -c $sql 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return ($out | Out-String).Trim()
}

function ConsumerStatus {
    # "running", "restarting", "exited", ... or "" when the container does not exist
    $s = docker inspect -f '{{.State.Status}}' $Consumer 2>$null
    if ($LASTEXITCODE -ne 0) { return "" }
    return ($s | Out-String).Trim()
}

function ConsumerLog {
    # Log of the current run only (since the last (re)start of the container).
    $since = (docker inspect -f '{{.State.StartedAt}}' $Consumer 2>$null | Out-String).Trim()
    if (-not $since) { return "" }
    return (cmd /c "docker logs --since $since $Consumer 2>&1" | Out-String)
}

function ConsumerReady { return ((ConsumerStatus) -eq "running") -and ((ConsumerLog) -match $ReadyText) }

function StartConsumer {
    # Starts stream-consumer (and Kafka + Postgres it depends on) and waits
    # until both streaming queries run. Returns $true when ready.
    if (ConsumerReady) { return $true }
    docker compose up -d $Consumer 2>$null | Out-Null
    Write-Host -NoNewline "  Menunggu streaming job siap"
    for ($i = 0; $i -lt 75; $i++) {
        Start-Sleep -Seconds 2
        Write-Host -NoNewline "."
        if (ConsumerReady) { Write-Host " siap."; return $true }
    }
    Write-Host ""
    Bad "Streaming job tidak siap dalam 150 detik. Akhir log ${Consumer}:"
    cmd /c "docker logs --tail 30 $Consumer 2>&1"
    return $false
}

function GhostCount {
    $r = docker compose exec -T airflow-scheduler bash -c "$FindGhosts | wc -l" 2>$null
    if ($LASTEXITCODE -ne 0) { return 0 }   # scheduler not running: no ghosts there
    return [int](($r | Out-String).Trim())
}

function KillGhosts {
    $n = GhostCount
    if ($n -gt 0) {
        docker compose exec -T airflow-scheduler bash -c "$FindGhosts | xargs -r kill" 2>$null | Out-Null
        Start-Sleep -Seconds 3
        Note "$n streaming job lama di airflow-scheduler dihentikan (sekarang hanya $Consumer)."
    }
}

function LastEvent {
    return Psql "SELECT COALESCE(to_char(max(observed_at), 'YYYY-MM-DD HH24:MI:SS'), '') FROM core.observations_stream"
}

# ================================================================== -Stop
if ($Stop) {
    Step "Menghentikan streaming job"
    docker compose stop $Consumer 2>$null | Out-Null
    KillGhosts
    Write-Host "Streaming job dihentikan. Jalankan .\demo.ps1 lagi untuk menyalakannya."
    exit 0
}

# ================================================================== -Check
if ($Check) {
    Step "Pemeriksaan pra-demo"
    $failed = 0

    docker compose up -d kafka postgres 2>$null | Out-Null
    $k = (docker inspect -f '{{.State.Health.Status}}' kafka 2>$null | Out-String).Trim()
    for ($i = 0; $i -lt 30 -and $k -ne "healthy"; $i++) {
        Start-Sleep -Seconds 2
        $k = (docker inspect -f '{{.State.Health.Status}}' kafka 2>$null | Out-String).Trim()
    }
    if ($k -eq "healthy") { Ok "Kafka berjalan" } else { Bad "Kafka tidak sehat (status: $k)"; $failed++ }

    $last = LastEvent
    if ($null -eq $last) { Bad "Postgres tidak bisa diquery"; $failed++ } else { Ok "Postgres bisa diquery" }

    KillGhosts
    if ((GhostCount) -eq 0) { Ok "Tidak ada streaming job lama di airflow-scheduler" }
    else { Bad "Masih ada streaming job lama di airflow-scheduler"; $failed++ }

    $wasReady = ConsumerReady
    if (StartConsumer) {
        $rc = (docker inspect -f '{{.RestartCount}}' $Consumer 2>$null | Out-String).Trim()
        Ok ("Streaming job berjalan di $Consumer" + $(if ($wasReady) { "" } else { " (baru dinyalakan)" }) +
            $(if ([int]$rc -gt 0) { "; pernah restart otomatis $rc kali" } else { "" }))
    } else { $failed++ }

    $producers = @(docker ps -q --filter "label=com.docker.compose.service=producer" 2>$null | Where-Object { $_ })
    if ($producers.Count -eq 0) { Ok "Tidak ada producer yang sedang mengirim" }
    else { Note "$($producers.Count) producer sedang mengirim data (demo lain masih berjalan?)" }

    if ($null -ne $last) {
        if ($last -eq "") { Ok "Tabel streaming kosong: demo akan mulai dari awal dataset (2022)" }
        elseif ($last -ge $DatasetEnd) { Bad "Seluruh dataset sudah diputar (event terakhir $last). Jalankan .\demo.ps1 -Reset"; $failed++ }
        else {
            $left = Psql "SELECT count(*) FROM core.observations WHERE observed_at > '$last'"
            Ok "Event terakhir $last; masih ada $left event untuk demo"
        }
    }

    Write-Host "`n  Memori container:" -ForegroundColor Gray
    docker stats --no-stream --format "    {{.Name}}: {{.MemUsage}}" 2>$null

    if ($failed -eq 0) {
        Write-Host "`nSiap demo. Jalankan: .\demo.ps1 -Limit 1000 -BurstMax 50" -ForegroundColor Green
        exit 0
    }
    Write-Host "`n$failed pemeriksaan gagal (lihat di atas)." -ForegroundColor Red
    exit 1
}

# ================================================================== -Reset
if ($Reset) {
    Step "Reset: mengosongkan tabel streaming dan checkpoint"
    docker compose stop $Consumer 2>$null | Out-Null
    KillGhosts
    docker compose up -d postgres 2>$null | Out-Null
    if ($null -eq (Psql "TRUNCATE core.observations_stream, mart.road_hourly_stream")) {
        Bad "TRUNCATE gagal (Postgres belum siap?). Coba lagi sebentar lagi."
        exit 1
    }
    Remove-Item data\checkpoints\traffic_stream, data\checkpoints\traffic_stream_hourly -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "Tabel streaming kosong; replay mulai lagi dari awal dataset."
}

# ================================================================== 1. streaming job
Step "Memastikan streaming job berjalan"
KillGhosts
$fresh = -not (Test-Path data\checkpoints\traffic_stream)
if (-not (StartConsumer)) { exit 1 }
if ($fresh) {
    # A new checkpoint starts at the Kafka offset that is "latest" when the
    # first micro-batch runs; give it that moment so the first events sent
    # below are not skipped.
    Start-Sleep -Seconds 8
}
Ok "Streaming job berjalan di $Consumer"

# ================================================================== 2. producer
Step "Mengirim data"
$last = LastEvent
if ($null -eq $last) { Bad "Postgres tidak bisa diquery."; exit 1 }
if ($last -ne "" -and $last -ge $DatasetEnd) {
    Note "Seluruh dataset sudah diputar (event terakhir $last). Jalankan .\demo.ps1 -Reset untuk mulai lagi dari 2022."
    exit 0
}

$IntervalArg = $Interval.ToString([System.Globalization.CultureInfo]::InvariantCulture)
$Pacing = if ($BurstMax -gt 0) { @("--burst-min", $BurstMin, "--burst-max", $BurstMax) } else { @("--interval", $IntervalArg) }
$PacingText = if ($BurstMax -gt 0) { "$BurstMin-$BurstMax event acak per detik" } else { "1 event tiap $Interval detik" }
$Common = @("python", "producer.py", "--topic", "traffic_topic", "--bootstrap-servers", "kafka:9092") + $Pacing + @("--limit", $Limit)

if ($last -eq "") {
    Write-Host "Mulai dari awal dataset. $Limit event, $PacingText."
    docker compose run --rm producer @Common
} else {
    $start = Psql "SELECT to_char(max(observed_at) + interval '1 second', 'YYYY-MM-DD HH24:MI:SS') FROM core.observations_stream"
    Write-Host "Melanjutkan setelah event terakhir ($last). $Limit event, $PacingText."
    docker compose run --rm producer @Common --start "$start"
}

Write-Host "`nSelesai. Streaming job tetap berjalan (micro-batch tiap 10 detik, sisa event masuk sebentar lagi)." -ForegroundColor Green
Write-Host "Tambah data: .\demo.ps1 lagi  |  hentikan: .\demo.ps1 -Stop"