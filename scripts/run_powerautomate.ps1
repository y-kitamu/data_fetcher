# Power Automate for desktopのフローを実行するPowerShellスクリプト
#   @param [string] $flowName 実行するフロー名
#   @param [bool] $flgExit PAD終了フラグ
#
#

Param(
  [parameter(mandatory=$true)][string]$flowName
)

#Power Automate for desktop起動
Start-Process -FilePath "ms-powerautomate://"

Set-Location C:\Users\kitamura\work\stock
poetry run python scripts/run_powerautomate.py $flowName
