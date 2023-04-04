from src.application.response import ResponseBot
class BotController:

    def __init__(self):
        pass
    def enviarController(self):
        response = ResponseBot() 
        dataresp = response.responseMostrar() 
        return dataresp
    
