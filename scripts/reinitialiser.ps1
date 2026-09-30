<#
    Supprime conteneurs ET volumes (lac, tables, topics Kafka, runs MLflow) :
    retour à zéro complet. Le code du projet n'est pas touché.
    Équivalent PowerShell de reinitialiser.sh.

    Utilisation :  .\scripts\reinitialiser.ps1
#>
#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\_commun.ps1"

Enter-RacineProjet
Assert-Docker

Invoke-Compose -Etape 'Suppression des conteneurs et volumes' -Arguments @('down', '-v')

Write-Host 'Environnement réinitialisé. Relancer : docker compose up -d'
