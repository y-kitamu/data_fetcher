import argparse
import time

import uiautomation as auto


def run_flow(flow_name: str, wait_until_finish: bool = False):
    control = auto.WindowControl(ClassName="WinAutomationWindow")
    while not control.Exists():
        time.sleep(1)
        control = auto.WindowControl(ClassName="WinAutomationWindow")
    control.SetFocus()

    data_item_ctl = control.DataItemControl(Name=flow_name)
    data_cell = data_item_ctl.CustomControl(Name=flow_name)
    exe_button = data_cell.ButtonControl(AutomationId="StartFlowButton")

    exe_button.Click()

    if wait_until_finish:
        while not exe_button.IsEnabled:
            time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("FlowName")

    args = parser.parse_args()

    run_flow(args.FlowName)
