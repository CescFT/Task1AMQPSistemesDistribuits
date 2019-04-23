'''
Created on 20 de mar. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''
import ast, pika
from reducer import reducer
from cos_backend import COS_Backend
from CountWords import countWords


def main(args):
	global url
	global connection
	global channel
	global nMaps
	global cos
	global llista_maps
	llista_maps=[]
	credencials=args.get("credentials")
	amqp=args.get("url")
	url = amqp['url']
	nMaps=args.get("num_maps")
	cos=COS_Backend(credencials)
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue='hello') 
	channel.queue_declare(queue='orchestrator')
	
	def on_message(channel, method_frame, header_frame, body):
		channel.basic_ack(delivery_tag=method_frame.delivery_tag)
		final=0
		ll=cos.list_objects('contenidortask1','wordcount')
		print(len(ll))
		if len(ll) == nMaps:
			final = 1
		else:
			final = 0
		print(body.decode('latin1'))
		print(str(body))
		print(str(body.decode('latin1')))
		
		
		if final == 1:
			print(llista_maps)
			i=0
			while i < nMaps:
				dades=ll[i]
				nom=dades['Key']
				contingut=cos.get_object('contenidortask1', nom)
				contingut_string=contingut.decode("utf-8")
				contingut_diccionari=ast.literal_eval(contingut_string)
				llista_maps.append(contingut_diccionari)
				i+=1
			diccionari_param_entrada={'merge':llista_maps}
			resultat=reducer(diccionari_param_entrada)
			cos.put_object('contenidortask1','resultat_finalAMQP.txt', str(resultat))
			
			diccionari_param={'merge':resultat}
			total_words=countWords(diccionari_param)
			cos.put_object('contenidortask1', 'countwordsAMQP.txt', str(total_words))
			channel.stop_consuming()
	
	channel.basic_consume(on_message, queue='hello')
	channel.start_consuming()
	channel.basic_publish(exchange='', routing_key='orchestrator', body='stop')
	connection.close()
	return {'ok':'ok'}