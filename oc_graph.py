from pymongo import MongoClient
from pprint import pprint
from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://localhost:7687',auth=('neo4j','pataton'))

client = MongoClient('localhost',27017)

db = client.dataton2019
buyers = db.buyers
tenderers = db.tenderers
ocds_data = db.cp # db.contrataciones


def add_party(tx, party, roles):
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

    if roles == 'buyer':
        try:
            query = "CREATE (b:Buyer {id:$id, name:$name, roles:$roles})"
            tx.run(query, id= party.get('id'), name= party.get('name'), roles = roles)
        except:
            pprint(party)
    else:
        try:
            query = "CREATE (p:Party {id:$id, name:$name, roles:$roles})"
            tx.run(query, id= party.get('id'), name= party.get('name') ,roles = roles)
        except:
            pprint(party)

def add_cp(tx, cp):
    ocid = cp.get('ocid')

    try:
        # create node
        query = "CREATE (cp:CP { ocid: $ocid, title: $title, procurementMethod: $pm})"
        tx.run(query, ocid = cp.get('ocid'), title = cp['tender']['title'], pm = cp['tender']['procurementMethod'])
    except:
        pprint(cp.get('ocid'))

    # create relations
    buyer_id = cp['buyer']['id']
    query= "MATCH(b:Buyer {id: $buyer_id}), (cp:CP {ocid: $ocid}) " \
           "CREATE (b)-[:BOUGHT]->(cp)"
    tx.run(query, buyer_id=buyer_id, ocid = ocid)

    parties = cp.get('parties')
    for p in parties:
        if 'tenderer' or 'supplier' in p.get('roles'):
            try:
                #pprint (ocid + ' -> '+ p.get('id'))
                query = "MATCH(p:Party {id: $party_id }), (cp: CP {ocid: $ocid}) " \
                        "CREATE (p)-[:PARTICIPATED]->(cp)"
                tx.run(query, party_id=p.get('id'), ocid=ocid)
            except:
                pprint(p)


with driver.session() as session:

    for b in buyers.find({}):
            session.write_transaction(add_party, b, 'buyer')

    for t in tenderers.find({}):
            session.write_transaction(add_party, t, 'tenderer')

    for cp in ocds_data.find({}):
        session.write_transaction(add_cp, cp)


session.close()
client.close()