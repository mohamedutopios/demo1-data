<#
    Pipeline DataOps minimal, exécuté comme le ferait une CI :
      1) reconstruit bronze → silver → gold ;
      2) lance les tests de données ;
      3) s'arrête en erreur si un test échoue.
    Équivalent PowerShell de ci_local.sh.

    Utilisation :  .\scripts\ci_local.ps1
    Code de sortie : 0 si tout passe, 1 sinon — c'est ce que lit une CI.
#>
#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\_commun.ps1"

Enter-RacineProjet
Assert-Docker

try {
    Write-Host '== 1/2 Construction bronze → silver → gold =='
    Invoke-Compose -Etape 'Construction du pipeline' -Arguments @(
        'exec', '-T', '-e', 'DEMO_SANS_PAUSE=1', 'spark',
        'python3', 'demos/03_pipeline_batch.py')

    Write-Host '== 2/2 Tests de données =='
    Invoke-Compose -Etape 'Tests de données' -Arguments @(
        'exec', '-T', '-e', 'DEMO_SANS_PAUSE=1', 'spark',
        'python3', 'demos/09_tests_qualite.py')
}
catch {
    Write-Host ''
    Write-Host "== Pipeline rouge : publication bloquée ==" -ForegroundColor Red
    Write-Host "   $($_.Exception.Message)"
    exit 1
}

Write-Host '== Pipeline vert : publication autorisée ==' -ForegroundColor Green
exit 0
