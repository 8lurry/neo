import json
from py2neo import Graph
from py2neo.bulk import create_nodes, merge_nodes, create_relationships, merge_relationships

# I have a neo4j docker container running with auth disabled and exposed on:
url = 'http://localhost:7474'       # Container's volume in mounted on the $PROJECT_DIR/data (for persistancy)
file = 'data.json'

class DoNeo(object):
    def __init__(self, exec_default=False):
        if exec_default:
            self.connect_db(url)
            self.load_data(file)
            self.populate_db()

    def connect_db(self, url=None):
        if url:
            self.g = Graph(url)
        else:
            self.g = Graph()

    def load_data(self, file):
        with open(file) as f:
            self.data = json.load(f)

    def populate_db(self):
        for d in self.data:
            d['Property']['IdUnique'] = d['IdUnique']
            if d['Kind'] == 'node':
                if d['DeDuplication'] == None and self.g.nodes.match(IdUnique=d['IdUnique']).count() == 0:
                    create_nodes(self.g.auto(), [d['Property']], labels={*d['Label']})
                else:
                    merge_nodes(self.g.auto(), [d['Property']], (tuple(d['Label']), 'IdUnique'), labels={*d['Label']})
            else:
                da = ((d['FromIdMaster']), d['Property'], (d['ToIdMaster']))
                if d['DeDuplication'] == None and self.g.relationships.match(IdUnique=d['IdUnique']).count() == 0:
                    create_relationships(self.g.auto(), [da], d['Type'], start_node_key=(d['FromLabel'], 'IdMaster'), end_node_key=(d['ToLabel'], 'IdMaster'))
                else:
                    merge_relationships(self.g.auto(), [da], (d['Type'], 'IdUnique'), start_node_key=(d['FromLabel'], 'IdMaster'), end_node_key=(d['ToLabel'], 'IdMaster'))


if __name__=="__main__":
    neodb = DoNeo(exec_default=True).g
