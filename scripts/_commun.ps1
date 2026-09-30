<#
    Fonctions communes aux scripts PowerShell du kit.
    Chargé par les autres scripts avec :  . "$PSScriptRoot\_commun.ps1"

    Pendant du fichier demos/common/lib.py côté Python.
#>

# Les sorties du kit contiennent des accents et des caractères comme ✔, » et ═.
# Sans cette ligne, la console Windows (code page 850 ou 1252 par défaut) les
# remplace par des points d'interrogation ou des symboles incohérents.
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)


function Assert-Docker {
    <#
        Vérifie que la commande docker existe ET que le démon répond, AVANT de
        lancer quoi que ce soit. Sans ce contrôle, l'échec se manifeste par un
        message technique illisible au milieu du script.
    #>
    $cmd = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $cmd) {
        throw "Docker est introuvable. Installez Docker Desktop, puis rouvrez ce terminal."
    }
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker ne répond pas. Lancez Docker Desktop, attendez qu'il soit démarré, puis réessayez."
    }
}


function Invoke-Compose {
    <#
        Lance « docker compose » et s'arrête net si la commande échoue.

        PowerShell n'interrompt PAS un script quand un programme externe renvoie
        un code non nul : il faut tester $LASTEXITCODE explicitement. C'est
        indispensable ici — sans cela, ci_local.ps1 afficherait « pipeline vert »
        alors que les tests de données viennent d'échouer.
    #>
    param(
        [Parameter(Mandatory, ValueFromRemainingArguments)]
        [string[]] $Arguments,

        [string] $Etape = 'commande docker compose'
    )
    docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Etape : échec (code de sortie $LASTEXITCODE)."
    }
}


function Enter-RacineProjet {
    # Les scripts sont dans scripts/ ; docker compose doit tourner à la racine.
    Set-Location (Join-Path $PSScriptRoot '..')
}
