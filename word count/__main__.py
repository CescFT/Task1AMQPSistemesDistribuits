'''
Created on 20 de mar. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''
import pika
from WordCount import wordCount
from cos_backend import COS_Backend
def main(args):
	credencials=args.get("credentials")
	text=args.get("text")
	index=args.get("index")
	amqp=args.get("url")

	cos=COS_Backend(credencials)
	
	diccionari_param_entrada={'text':text}
	resultat=wordCount(diccionari_param_entrada)
	cos.put_object('contenidortask1','wordcount'+str(index)+'.txt', str(resultat))
	
	url = amqp['url']
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel() 
	channel.queue_declare(queue='hello')
	message='wordcount'+str(index)+'.txt'
	print (message)
	channel.basic_publish(exchange='', routing_key='hello', body=message)
	connection.close()
	return {'ok':'ok'}