<#
    Crée le topic « tickets » (3 partitions, rétention 7 jours) puis l'affiche.
    Équivalent PowerShell de creer_topic.sh.

    Utilisation :  .\scripts\creer_topic.ps1
#>
#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\_commun.ps1"

Enter-RacineProjet
Assert-Docker

$topics = 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
          '--bootstrap-server', 'localhost:9092'

Invoke-Compose -Etape 'Création du topic tickets' -Arguments (
    $topics + @('--create', '--if-not-exists', '--topic', 'tickets',
                '--partitions', '3', '--replication-factor', '1',
                '--config', 'retention.ms=604800000'))

Invoke-Compose -Etape 'Description du topic tickets' -Arguments (
    $topics + @('--describe', '--topic', 'tickets'))
