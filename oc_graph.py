from pymongo import MongoClient
from pprint import pprint
from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://localhost:7687',auth=('neo4j','pataton'))

client = MongoClient('localhost',27017)

db = client.dataton2019
buyers = db.buyers
parties = db.tenderers
ocds_data = db.cp # db.contrataciones


def add_party(tx, party, type):
    '''
    #pprint(party.get('id'))
    pid = party.get('id')
    pid = pid.replace('-', '_')
    pid = pid.replace(' ', '')
    pid = pid.replace('&','')
    pid = pid.replace('/','')
    pid = pid.replace('"','')
    pid = pid.replace(',','')
    pid = pid.replace('.','')
    pid = pid.replace('(','')
    pid = pid.replace(')','')
    pid = pid.replace('+','')
    pid = pid.replace('\t','')
    pid = pid.replace('\xa0', '')
    pid = pid.replace(':', '')
    pid = pid.replace('{', '')
    '''

    if type == 'buyer':
        try:
            query = "CREATE (b:Buyer {id:$id, name:$name})"
            tx.run(query, id= party.get('id'), name= party.get('name'))
        except:
            pprint(party)
    else:
        try:
            query = "CREATE (p:Party {id:$id, name:$name})"
            tx.run(query, id= party.get('id'), name= party.get('name'))
        except:
            pprint(party)

def add_cp(tx, cp):
    ocid = cp.get('ocid')

    # contracts data
    contracts_data = cp.get('contracts',None)
    contracts = []
    total_amount = 0

    if contracts_data is not None:
        for c in contracts_data:
            total_amount += c['value']['amount']
            contracts.append(c['value']['amount'])
    else:
        pprint('No contacts data -> '+ cp['ocid'])

    # create node
    try:
        query = "CREATE (cp:CP { ocid: $ocid, title: $title, procurementMethod: $pm, totalAmount: $total_amount, contracts: $contracts})"
        tx.run(query, ocid = cp.get('ocid'), title = cp['tender']['title'], pm = cp['tender']['procurementMethod'], total_amount= total_amount, contracts= contracts)
    except:
        pprint(cp.get('ocid'))

    # create relations
    buyer_id = cp['buyer']['id']
    query= "MATCH(b:Buyer {id: $buyer_id}), (cp:CP {ocid: $ocid}) " \
           "CREATE (b)-[:BOUGHT {roles: $roles}]->(cp)"
    tx.run(query, buyer_id=buyer_id, ocid = ocid, roles= ["buyer"])

    parties = cp.get('parties')
    for p in parties:
        if 'tenderer' or 'supplier' in p.get('roles'):
            try:
                #pprint (ocid + ' -> '+ p.get('id'))
                query = "MATCH(p:Party {id: $party_id }), (cp: CP {ocid: $ocid}) " \
                        "CREATE (p)-[:PARTICIPATED {roles: $roles }]->(cp)"
                tx.run(query, party_id=p.get('id'), ocid=ocid, roles = p.get('roles'))
            except:
                pprint(p)


with driver.session() as session:

    for b in buyers.find({}):
            session.write_transaction(add_party, b, 'buyer')

    for p in parties.find({}):
            session.write_transaction(add_party, p, 'party')

    cursor = ocds_data.find({}) #.skip(226687)
    cursor.batch_size(10)

    for cp in cursor:
        session.write_transaction(add_cp, cp)

session.close()
client.close()