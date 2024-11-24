import os
import asyncio
import time
import traceback
import json
from slack_sdk import WebClient
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.aio import AutoLockRenewer
from azure.servicebus import ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

def get_secret(secret_name):    
    tenant_id = os.environ["vault_tenant_id"] 
    client_id = os.environ["vault_client_id"] 
    secret =  os.environ["vault_secret"] 
    credential = ClientSecretCredential(tenant_id, client_id, secret, additionally_allowed_tenants=['*'])
    vault_url = os.environ["vault_url"] 
    client = SecretClient(vault_url=vault_url, credential=credential)
    secret_vault = client.get_secret(secret_name)  
    return secret_vault.value

def send_message_to_slack(message):
    slack_data = json.loads(get_secret('slack-creds'))
    client = WebClient(token=slack_data['token'])
    client.chat_postMessage(channel=slack_data['warning_channel'], text=message)

def get_dlq_topics(conn):     
    servicebus_client = ServiceBusAdministrationClient.from_connection_string(conn)
    final_list = []
    #iterate through all topic and subscriptions and get dead_letter_message_count
    for t in servicebus_client.list_topics():
        for s in servicebus_client.list_subscriptions(t["name"]):
                dead_letter_number = servicebus_client.get_subscription_runtime_properties(t["name"],s["name"]).dead_letter_message_count
                if dead_letter_number > 0:
                    final_dict = {}
                    final_dict["topic"] = t["name"]
                    final_dict["subscription"] = s["name"]
                    final_dict["needSession"] = s.requires_session
                    final_dict["qnt"] = dead_letter_number
                    final_list.append(final_dict.copy())
    return final_list

async def create_servicebus_sender_async(sb_conn_str, topic_name):  
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=sb_conn_str)
    topic_sender = servicebus_client.get_topic_sender(topic_name=topic_name)
    return topic_sender

async def create_servicebus_receiver_async(sb_conn_str, topic_name, subscription_name):  
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=sb_conn_str)
    subscription_receiver = servicebus_client.get_subscription_receiver(
            topic_name=topic_name,
            subscription_name=subscription_name,
            sub_queue= 'deadletter'
        )
    return subscription_receiver

async def resend_messages(topic_name, sub_name, connstr, need_session):
    sem = asyncio.Semaphore(10)
    async with sem:
        #we need dc for monitoring, at the end the notification with number of message will be send to slack
        dc = {}
        dc['topic'] = topic_name
        dc['subscirption'] = sub_name
        lock_renewal = AutoLockRenewer()
        servicebus_receiver = await create_servicebus_receiver_async(connstr, topic_name, sub_name)
        servicebus_sender = await create_servicebus_sender_async(connstr, topic_name)
        async with servicebus_receiver:       
            async with servicebus_sender:
                messages = await servicebus_receiver.receive_messages(max_message_count = 200)
                for message in messages:
                    lock_renewal.register(servicebus_receiver, message, max_lock_renewal_duration=200)
                    user_properties = {}
                    #get all message properties
                    for key, val in message.application_properties.items():
                        if key.decode()!='DeadLetterReason' and key.decode()!='DeadLetterErrorDescription':
                            if isinstance(val, bytes):
                                user_properties[key.decode()] = val.decode()
                            else:
                                user_properties[key.decode()] = val
                    #if enqueued_time_utc_original was found set it, other wise take enqueued_time_utc 
                    message_app_prop = {
                                        "sequence_number_original":user_properties.get("sequence_number_original", message.sequence_number), 
                                        "sub_name" : sub_name,
                                        "enqueued_time_utc_original":user_properties.get("enqueued_time_utc_original",message.enqueued_time_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
                                        }  
                    message_app_prop = {**message_app_prop, **user_properties}
                    #if subscription needs session set it 
                    if need_session == True:
                        sb_mmsg = ServiceBusMessage(str(message),subject=message.subject,session_id=message.session_id)
                    else:
                        sb_mmsg = ServiceBusMessage(str(message),subject=message.subject)
                    sb_mmsg.application_properties = message_app_prop            
                    await servicebus_sender.send_messages(sb_mmsg)
                    await servicebus_receiver.complete_message(message)
                    dc['qnt'] = dc.get('qnt',0) + 1   
        await lock_renewal.close()
    return dc  


async def do():    
    try:
        start_time = time.time()   
        tasks = []   
        connstr = get_secret('dwh-service-bus-conn-str') 
        dlq_topics = get_dlq_topics(connstr)
        if dlq_topics:  
            for i in dlq_topics:             
                tasks.append(asyncio.create_task(resend_messages(i['topic'],i['subscription'],connstr,i['needSession'])))
        result = await asyncio.gather (*tasks, return_exceptions=True)
        total_number = 0
        for i in result:
            total_number = total_number + i['qnt']     
        if dlq_topics:
            send_message_to_slack(f'Messages from DLQ ServiceBus were handled:   \n{json.dumps(result)}   \nTotalNumber: {total_number}   \nTimeInSec: {round((time.time() - start_time),1)}')
    except:
        send_message_to_slack(traceback.format_exc())
 
if __name__ == "__main__":
    asyncio.run(do ()) 






