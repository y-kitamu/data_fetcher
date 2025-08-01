import argparse
import datetime
import time

import uiautomation as auto
from loguru import logger


def run_flow(flow_name: str, wait_until_finish: bool = False):
    # power automateのwindowにフォーカスをあてる
    control = auto.WindowControl(ClassName="WinAutomationWindow")
    while not control.Exists():
        time.sleep(1)
        control = auto.WindowControl(ClassName="WinAutomationWindow")
    control.SetFocus()

    time.sleep(2)

    # フロータブをクリック
    control.SetFocus()
    flow_tab = control.TabItemControl(AutomationId="FlowsTab")
    while not flow_tab.Exists() or not flow_tab.IsEnabled:
        time.sleep(1)
        flow_tab = control.TabItemControl(AutomationId="FlowsTab")
    control.SetFocus()
    flow_tab.Click()

    time.sleep(2)

    # 実行ボタンをクリック
    control.SetFocus()
    data_item_ctl = control.DataItemControl(Name=flow_name)
    data_cell = data_item_ctl.CustomControl(Name=flow_name)
    icon = data_cell.TextControl(AutomationId="PART_IconText")
    icon.Click()
    time.sleep(1)
    exe_button = data_cell.ButtonControl(AutomationId="StartFlowButton")
    while not exe_button.Exists() or not exe_button.IsEnabled:
        time.sleep(1)
        data_item_ctl = control.DataItemControl(Name=flow_name)
        data_cell = data_item_ctl.CustomControl(Name=flow_name)
        exe_button = data_cell.ButtonControl(AutomationId="StartFlowButton")
    control.SetFocus()
    exe_button.Click()

    if wait_until_finish:
        while not exe_button.IsEnabled:
            time.sleep(60)


if __name__ == "__main__":
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    logger.add(
        f"logs/run_powerautomate_{date_str}.log",
        format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file} at line {line}] {message}",
        level="DEBUG",
    )
    logger.debug("Starting Power Automate flow...")

    parser = argparse.ArgumentParser()
    parser.add_argument("FlowName")

    args = parser.parse_args()

    run_flow(args.FlowName)

    logger.debug("Power Automate flow finished.")
