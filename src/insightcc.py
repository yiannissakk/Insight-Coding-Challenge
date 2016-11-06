import json
import statistics
from datetime import datetime
from collections import defaultdict


def parser(filename):
    
    #max_time is the maximum timestamp that has been processed
    max_time= ''
    
    #the following dictionary will be of the form "created_time":["actor","target"]
    time_dict = defaultdict(list)
    
    #the following dictionary will be of the form "actor:target":["created_time"] or "target:actor":["created_time"]
    users_dict = defaultdict(str)
    
    #create/open output.txt file to write the rolling median
    g = open('./venmo_output/output.txt', 'w')
    
    #open file containing json data
    with open(filename) as f:
        
        #go through each json line
        for line in f:
            current_transaction = (json.loads(line))
            
            #catch any errors, they can only occur inside the new_entry method
            try:
                [max_time,time_dict,users_dict] = new_entry(current_transaction, max_time, time_dict, users_dict)
                
                #the following dictionary will be of the form 'userX': number of other users, userX has had a transaction inside the 60 second interval of max_time
                occ_dict = defaultdict(int)
                #for every transaction, compute occ_dict[user]
                for pair in users_dict:
                    user1 = pair.partition(':')[0]
                    user2 = pair.partition(':')[2]
                    occ_dict[user1] = float(occ_dict[user1]) + 1.0
                    occ_dict[user2] = float(occ_dict[user2]) + 1.0
                    
                #enter the median at each incoming transaction
                g.write('%.2f'%statistics.median(occ_dict.values())+'\n')
                
            #if any errors caught, go to the next transaction
            except:
                pass

    f.close()

def new_entry(curr_trans, mx_time, time_dct, users_dct):
    
    #first transaction processed only
    if mx_time == '':
        mx_time = curr_trans['created_time']
        users_dct[curr_trans['target']+':'+curr_trans['actor']] = curr_trans['created_time']
        time_dct[curr_trans['created_time']]=[[curr_trans['actor'],curr_trans['target']]]
        return [mx_time, time_dct, users_dct]
    
    else: #any transaction after the first
        
        #in order to compare the timestamp strings easily, they are converted to datetime objects
        incoming_time_object = datetime.strptime(curr_trans['created_time'], "%Y-%m-%dT%H:%M:%SZ")
        maximum_time_object = datetime.strptime(mx_time, "%Y-%m-%dT%H:%M:%SZ")
        
        #new transaction inOrder not same time as mx_time, here the mx_time changes, thus we need to delete entries that are older than one minute from mx_time
        if maximum_time_object < incoming_time_object :
            
            #update mx_time and time_dct
            mx_time = curr_trans['created_time']
            maximum_time_object = datetime.strptime(mx_time, "%Y-%m-%dT%H:%M:%SZ")
            time_dct[curr_trans['created_time']] = [[curr_trans['actor'],curr_trans['target']]]

            #avoid duplication of transaction pairs in users_dct
            if (curr_trans['actor']+':'+curr_trans['target']) in users_dct:
                users_dct[curr_trans['actor']+':'+curr_trans['target']] = curr_trans['created_time']
            else:
                users_dct[curr_trans['target']+':'+curr_trans['actor']] = curr_trans['created_time']

            #delete all entries in time_dct that are more than a minute older than the new mx_time
            A=[]
            for time in time_dct:
                time_object = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
                if (maximum_time_object - time_object).seconds > 60:
                    A.append(time)
            for t in A:
                del time_dct[t]

            #similarly delete all entries in users_dct that are more than a minute older than the new mx_time
            B=[]
            for pair in users_dct:
                pair_max_time_object = datetime.strptime(users_dct[pair], "%Y-%m-%dT%H:%M:%SZ")
                if (maximum_time_object - pair_max_time_object).seconds > 60:
                    B.append(pair)
            for p in B:
                del users_dct[p]
            
            return [mx_time, time_dct, users_dct]
                    
        elif maximum_time_object == incoming_time_object:#new transaction inOrder same time as max

            #avoid duplication of transaction pairs
            if (curr_trans['actor']+':'+curr_trans['target']) in users_dct:
                users_dct[curr_trans['actor']+':'+curr_trans['target']] = curr_trans['created_time']
            else:
                users_dct[curr_trans['target']+':'+curr_trans['actor']] = curr_trans['created_time']

            #update time_dct
            time_dct[curr_trans['created_time']].append([curr_trans['actor'],curr_trans['target']])
            
            return [mx_time, time_dct, users_dct]
        
        else: #new transaction outOfOrder
            
            #the transaction is inside the one minute mark from mx_time
            if (maximum_time_object-incoming_time_object).seconds <= 60:

                #update
                time_dct[curr_trans['created_time']].append([curr_trans['actor'],curr_trans['target']])

                #avoid duplication of transaction pairs in users_dct
                if (curr_trans['actor']+':'+curr_trans['target']) in users_dct:
                    pair_time_object_at = datetime.strptime(users_dct[curr_trans['actor']+':'+curr_trans['target']],"%Y-%m-%dT%H:%M:%SZ") 
                    if pair_time_object_at < incoming_time_object:
                        users_dct[curr_trans['actor']+':'+curr_trans['target']] = curr_trans['created_time']
                elif (curr_trans['target']+':'+curr_trans['actor']) in users_dct:
                    pair_time_object_ta = datetime.strptime(users_dct[curr_trans['target']+':'+curr_trans['actor']],"%Y-%m-%dT%H:%M:%SZ") 
                    if pair_time_object_ta < incoming_time_object:
                        users_dct[curr_trans['target']+':'+curr_trans['actor']] = curr_trans['created_time']
                else:
                    users_dct[curr_trans['target']+':'+curr_trans['actor']] = curr_trans['created_time']

            #the transaction is not inside the one minute mark from mx_time
            else:
                
                #I make the following variable assignments in order for the try statement in the parser method to catch a possible error
                actor = curr_trans['actor']
                target = curr_trans['target']
                created_time = curr_trans['created_time']
                        
            return [mx_time, time_dct, users_dct]
                
            
        
    
parser('./venmo_input/venmo-trans.txt')
