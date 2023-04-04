import requests
from src.mongo.connect import ConnectionMongo
from pymongo import MongoClient
from datetime import datetime
import pytz
import json
class ResponseBot:
    def __init__(self):
        pass
    def responseMostrar(self):
        listaempresas = self.listarEmpresas()
        payloadrutinas = []
        for empresa in listaempresas:
            if empresa["ruc"] != "1716024474001":
                print("ACA EMPIEZA A ANALIZAR UNA EMPRESA: " + empresa['empresa'])
                listrutas = self.conseguiridroute(empresa['token'], empresa['depot'])
                for dataruta in listrutas:
                    respApi = self.consumirApiMinutos(empresa['token'], empresa['depot'], dataruta["id"])
                    listrutinasEnviar = self.parsearDataRutinaEnviar(respApi, dataruta["nombre"], empresa["ruc"])
                    payloadrutinas = payloadrutinas + listrutinasEnviar
        payloadenviar = self.validarRutinasMongo(payloadrutinas)
        resultenviar = self.insertarRutinasMongo(payloadenviar)
        respuesta = []
        for resp in resultenviar.inserted_ids:
            respuesta.append(resp)
        return respuesta

    def consumirApiMinutos(self, token, depot, idroute):
        headers = {
            'Content-Type' : 'application/json',
            'Authorization': f'Token {token}'
        }
        result = requests.get(f"https://nimbus.wialon.com/api/depot/{depot}/report/route/{idroute}?flags=1&df=03.04.2023&dt=04.04.2023&sort=timetable", headers= headers)
        resp = result.json()
        return resp
    
    def conseguiridroute(self, token, depot):
        headers = {
            'Authorization': f'Token {token}'
        }
        result = requests.get(f"https://nimbus.wialon.com/api/depot/{depot}/routes", headers= headers)
        resp = result.json()
        listrutas = []
        for ruta in resp["routes"]:
            objeruta = {}
            objeruta["nombre"] = ruta["n"]
            objeruta["id"] = ruta["id"]
            listrutas.append(objeruta)
        return listrutas
    
    def listarEmpresas(self):
        connect = ConnectionMongo()
        db = connect.con
        col = db["tbcliente"]
        docs = col.find({}, {'_id': False})
        resp = []
        for doc in docs:
            dicc = {}
            if doc['status'] == True:
                dicc['empresa'] = doc['empresa']
                dicc['token'] = doc['token']
                dicc['depot'] = doc['depot']
                dicc['ruc'] = doc['ruc']
                resp.append(dicc)
        return resp
    
    def parsearDataRutinaEnviar(self, resApi, nameruta, ruc):
        listRutinasEnviar = []
        for rutina in resApi["report_data"]["rows"]:
            if rutina["cols"][0]["t"] != "—":
                objerutina = {}
                objerutina["ruta"] = nameruta
                objerutina["ruc"] = ruc
                objerutina["rutina"] = rutina["cols"][3]["t"]
                objerutina["placa"] = rutina["cols"][0]["t"].replace(" ","")[-9:]
                fecha_nimbus = datetime.strptime(rutina["cols"][1]["v"], "%Y-%m-%d")
                fecha_nueva = fecha_nimbus.strftime("%d-%m-%Y")
                objerutina["fecha"] = str(fecha_nueva)
                objerutina["fechaunix"] = int(datetime.strptime(fecha_nueva, "%d-%m-%Y").timestamp())
                identificador = str(objerutina['ruta']) + (objerutina['rutina']) + str(objerutina['placa']) + str(objerutina['fechaunix'])
                objerutina['identificador'] = identificador.replace(" ","")
                rutinaparadas = []
                for parada in rutina["rows"]:
                    objparada = {}
                    objparada["parada"] = parada[0]["t"]
                    objparada["horaplanificada"] = parada[3]["t"]
                    if parada[4]["t"] == "—":
                        objparada["horaejecutada"] = "--:--"
                    else:
                        objparada["horaejecutada"] = parada[4]["t"]
                    if parada[7]["t"] == "—":
                        objparada["min"] = "-"
                    else:
                        objparada["min"] = str(parada[7]["t"])
                    rutinaparadas.append(objparada)
                objerutina['rutinaparadas'] = rutinaparadas
                listRutinasEnviar.append(objerutina)
        return listRutinasEnviar
    
    def insertarRutinasMongo(self, payload):
        connect = ConnectionMongo()
        db = connect.con
        col = db["report_minutosc"]
        results = col.insert_many(payload)
        print(results)
        return results
    
    def consultarRutinasMongo(self):
        rutinasmongo = []
        connect = ConnectionMongo()
        db = connect.con
        col = db["report_minutosc"]
        fecha1 = "03-04-2023"
        fecha2 = "04-04-2023"
        results = col.find({'$or': [{'fecha': {'$regex': fecha1}}, {'fecha': {'$regex': fecha2}}]}, {'_id': False})
        for result in results:
            rutinasmongo.append(result)
        return rutinasmongo
    
    def validarRutinasMongo(self, listpayload):
        rutinasmongo = self.consultarRutinasMongo()
        payloadLimpio = []
        insertados = 0
        repetidos = 0
        for rutina in listpayload:
            unic = 0
            for rutinamongo in rutinasmongo:
                if rutina['identificador'] == rutinamongo['identificador']:
                    repetidos += 1
                    unic += 1
            if unic == 0:
                insertados += 1
                payloadLimpio.append(rutina)
        print("Insertados: "+ str(insertados))
        print("Repetidos: "+ str(repetidos))
        return payloadLimpio

                
             
