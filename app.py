from src.infrastructure.controller import BotController
import json, time, datetime

def main():
    try:
        _botCL = BotController() 
        dataResp = _botCL.enviarController()
        print(dataResp)
    except Exception as err:
        print(err)

main()