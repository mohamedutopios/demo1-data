<#
    Répétition complète sans pause (environ 5 minutes, hors streaming).
    À lancer la veille : si ce script se termine en vert, la démo passera en salle.
    Équivalent PowerShell de repetition.sh.

    Utilisation :  .\scripts\repetition.ps1
#>
#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\_commun.ps1"

Enter-RacineProjet
Assert-Docker

# Chaque entrée : le script, puis ses éventuels arguments.
$etapes = @(
    @('demos/00_verification.py'),
    @('demos/01_generer_donnees.py'),
    @('demos/02_lac_schema_on_read.py'),
    @('demos/03_pipeline_batch.py'),
    @('demos/04_lakehouse_mecanismes.py', '--essentiel'),
    @('demos/09_tests_qualite.py')
)

try {
    foreach ($etape in $etapes) {
        Write-Host ''
        Write-Host ("════════ {0} ════════" -f ($etape -join ' '))
        Invoke-Compose -Etape ($etape -join ' ') -Arguments (
            @('exec', '-T', '-e', 'DEMO_SANS_PAUSE=1', 'spark', 'python3') + $etape)
    }
}
catch {
    Write-Host ''
    Write-Host "✘ Répétition interrompue : $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "  Corrigez avant d'entrer en salle."
    exit 1
}

Write-Host ''
Write-Host '✔ Répétition terminée : les 5 étapes (hors streaming) passent.' -ForegroundColor Green
Write-Host '  Le streaming se vérifie à part : .\scripts\creer_topic.ps1 puis les scripts 05, 06, 07.'
exit 0
