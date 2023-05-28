from datetime import date
from nltk.corpus import words
from nltk import *
from pydantic import BaseModel
class tarea1:
    path_:str
    words_:dict
    count_:int
    #adicionando algunos comentarios al documento
    def __init__(self,p,w,c):
        self.path_=p
        self.words_=w
        self.count_=c        
    
    #este metodo implementa la solicion para el punto numero 1
    #es leer un path, almacenar las words ( se validan a un diccionario) de nltk
    def primero(self,path):
        print("fecha de inicio ",date.today())
        self.words_={}
        self.path_=path
        file = open(path, "rt") 
        archivo_=file.read()
        self.fillWords(archivo_,archivo_.split())   
        self.count_=len(self.words_.keys())
        print ("total Words",len(self.words_.keys()))
        file.close()     
        
    def segundo(self,path):
        print("fecha de inicio ",date.today())
        self.path_=path
        file = open(path, "rt") 
        file_=file.read()
        words=file_.split()        
        self.fillWords(file_,words) 
        file.close()   
    
    def sort(self,top):
        sort_data = sorted(self.words_.items(), key=lambda x: x[1],reverse=True)
        sort_data_dict = dict(sort_data[:top])
        self.words_=sort_data_dict

    def tercero(self,path,top):
        self.segundo(path)
        self.sort(top)
        self.count_=len(self.words_)

    def fillWords(self,a,p):
        notwords={}
        setofwords=set(words.words())    
        for palabra in p:
            if(palabra not in notwords):
                if(palabra not in self.words_):
                    if(palabra.lower() in setofwords):
                        self.words_[palabra]=a.count(palabra)
                    else:
                        #print("no encontrada ",palabra)
                        notwords[palabra]=palabra
        self.count_=len(self.words_.keys())        
        return notwords
