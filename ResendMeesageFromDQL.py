import asyncio
import time
import traceback
import json
from slack_sdk import WebClient
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.aio import AutoLockRenewer
from azure.servicebus import ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient

def send_message_to_slack(message):
    WebClient(token="Youe slack token here").chat_postMessage(channel="Id of the channel", text=message)

def get_dlq_topics(conn):     
    servicebus_client = ServiceBusAdministrationClient.from_connection_string(conn)
    final_list = []
    #iterate through all topic and subscriptions and get dead_letter_message_count
    for t in servicebus_client.list_topics():
        for s in servicebus_client.list_subscriptions(t["name"]):
            dead_letter_number = servicebus_client.get_subscription_runtime_properties(t["name"],s["name"]).dead_letter_message_count
            if dead_letter_number > 0:
                final_dict = {"topic": t["name"], "subscription": s["name"], "needSession": s.requires_session, "qnt": dead_letter_number}
                final_list.append(final_dict.copy())
    return final_list

async def create_servicebus_sender_async(topic_name):  
    sb_client = ServiceBusClient.from_connection_string(conn_str="Your conn")
    return sb_client.get_topic_sender(topic_name=topic_name)
     
async def create_servicebus_receiver_async(topic_name, subscription_name):  
    sb_client = ServiceBusClient.from_connection_string(conn_str="Your conn")
    return sb_client.get_subscription_receiver(
        topic_name=topic_name,
        subscription_name=subscription_name,
        sub_queue= "deadletter"
        )

async def resend_messages(topic_name, sub_name, need_session):
    #we need dc for monitoring, at the end the notification with number of message will be send to slack
    dc = {"topic":topic_name, "subscirption": sub_name}
    lock_renewal = AutoLockRenewer()
    servicebus_receiver = await create_servicebus_receiver_async(topic_name, sub_name)
    servicebus_sender = await create_servicebus_sender_async(topic_name)
    async with servicebus_receiver, servicebus_sender:       
        messages = await servicebus_receiver.receive_messages(max_message_count = 200)
        for message in messages:
            lock_renewal.register(servicebus_receiver, message, max_lock_renewal_duration=200)
            #get all message properties
            for key, val in message.application_properties.items():
                if key.decode() in ["DeadLetterReason","DeadLetterErrorDescription"]:
                    continue
                user_properties = {key.decode(): (val.decode() if isinstance(val, bytes) else val)}
            #if enqueued_time_utc_original was found set it, other wise take enqueued_time_utc 
            message_app_prop = {"sub_name" : sub_name,
                                "enqueued_time_utc_original":user_properties.get("enqueued_time_utc_original",message.enqueued_time_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))}  
            message_app_prop = {**message_app_prop, **user_properties}
            sb_mmsg = ServiceBusMessage(str(message),
                                        subject=message.subject,
                                        session_id=message.session_id if need_session == True else None
                                        )
            sb_mmsg.application_properties = message_app_prop            
            await servicebus_sender.send_messages(sb_mmsg)
            await servicebus_receiver.complete_message(message)
            dc['qnt'] = dc.get('qnt',0) + 1   
    await lock_renewal.close()
    return dc  


async def do():    
    try:
        start_time = time.time()   
        dlq_topics = get_dlq_topics()
        if not dlq_topics: 
            return 
        tasks = [asyncio.create_task(resend_messages(i["topic"], i["subscription"], i["needSession"])) for i in dlq_topics]
        result = await asyncio.gather (*tasks, return_exceptions=True) 
        send_message_to_slack(f'Messages from DLQ ServiceBus were handled:   \n{json.dumps(result)}   \nTimeInSec: {round((time.time() - start_time),1)}')
    except:
        send_message_to_slack(traceback.format_exc())
 
if __name__ == "__main__":
    asyncio.run(do ()) 

