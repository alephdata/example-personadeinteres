import os
import json
from pathlib import Path
from pprint import pprint  # noqa

from followthemoney import model
from alephclient.api import AlephAPI


RESOURCES = {
    'personas': 'Person',
    'empresas': 'Company',
    'acusaciones': 'CourtCase',
    'propiedades': 'RealEstate',
}

DOC_SECTIONS = (
    'Documentos de registro',
    'Documentos OFAC',
    'Documentos',
    'Documento de propiedad',
    'Registro de propiedad',
)

PROPERTIES = {
    'Nombre': 'name',
    'Aka': 'alias',
    'Aliases': 'alias',
    'Apellido': 'firstName',
    'Fecha de incorporación': 'incorporationDate',
    'Fecha de nacimiento': 'birthDate',
    'Lugar de nacimiento': 'birthPlace',
    'Números de cédula': 'idNumber',
    'Otros direcciones': 'address',
    'Dirección registrada': 'address',
    'Ingreso anual': 'amount',
    'Países de operación': 'country',
    'Países de residencia': 'country',
    'Nacionalidades': 'nationality',
    'Información adicional': 'sourceUrl',
    'Tipos de negocio ilegal': 'classification',
    'Tipo': 'type',
    'Cargo': 'category',
    'Fecha': 'fileDate',
    'Descripción': 'summary',
    'Corte': 'court',
    'Dirección': 'address',
    'Valor estimado': 'amount',
    'Tipo de propiedad': 'propertyType',
    # Relation props
    'Connection': 'role',
    'Description': 'summary',
    'Tipo de participación': 'ownershipType',
    'Porcentaje de participación': 'percentage',
    'Percentage ownership': 'role',
}

RELATIONS = {
    'Asociado con': ('Associate', 'person', 'associate', 'relationship'),
    'Dueño de empresas': ('Ownership', 'owner', 'asset', 'role'),
    'Dueño de propiedades': ('Ownership', 'owner', 'asset', 'role'),
    # 'Tiene como dueño de empresa': ('Ownership', 'owner', 'asset', 'role'),
    # 'Tiene como dueño de propiedad': ('Ownership', 'asset', 'owner', 'role'),
    'Documentos relacionados': ('UnknownLink', 'subject', 'object', 'role'),
    # 'Relacionada con': ('UnknownLink', 'object', 'subject', 'role'),
    # 'Acusado por': ('CourtCaseParty', 'party', 'case', 'role'),
    'Acusado': ('CourtCaseParty', 'case', 'party', 'role'),
}


def upload_document(root_path, documents, api, cid, document, title=None):
    file_name, rel_path = document
    if rel_path in documents:
        return documents[rel_path]
    full_path = Path(os.path.join(root_path, rel_path))
    if not os.path.exists(full_path):
        return
    metadata = {'file_name': file_name, 'title': title}
    res = api.ingest_upload(cid, full_path, metadata)
    print("Uploaded [%s]: %r" % (file_name, res))
    documents[rel_path] = res.get('id')
    return res.get('id')


def make_node(root_path, documents, api, cid, entity):
    entity_id = entity.pop('id')
    resource, _ = entity_id.split('/')
    schema = RESOURCES.get(resource)
    if schema is None:
        return
    proxy = model.make_entity(schema)
    proxy.make_id(entity_id)
    proxy.add('name', entity.pop('title'))
    proxy.add('summary', entity.pop('abstract'))
    proxy.add('description', entity.pop('body'))
    for section, values in entity.items():
        for value in values:
            if section in DOC_SECTIONS:
                doc = upload_document(root_path, documents, api, cid, value)
                dproxy = model.make_entity('UnknownLink')
                dproxy.make_id(proxy.id, section, value)
                dproxy.add('subject', proxy.id)
                dproxy.add('object', doc)
                dproxy.add('role', section)
                yield dproxy
                continue
            if section == 'Organización o célula':
                org = model.make_entity('Organization')
                org.make_id('organization', value)
                org.add('name', value)
                yield org
                mem = model.make_entity('Membership')
                mem.make_id('membership', proxy.id, org.id)
                mem.add('organization', org)
                mem.add('member', proxy)
                mem.add('role', section)
                yield mem
                continue
            if section == 'Esposo o esposa':
                spouse = model.make_entity('Person')
                spouse.make_id('person', value)
                spouse.add('name', value)
                yield spouse
                family = model.make_entity('Family')
                family.make_id('family', proxy.id, spouse.id)
                family.add('person', proxy)
                family.add('relative', spouse)
                family.add('relationship', section)
                yield family
                continue
            prop = PROPERTIES.get(section)
            if schema == 'Person' and prop == 'name':
                prop = 'lastName'
            proxy.add(prop, value)
            if schema == 'Company' and prop == 'summary':
                proxy.add('jurisdiction', value, quiet=True)

    print(repr(proxy))
    yield proxy


def make_relation(root_path, documents, api, cid, entity):
    relation = entity.pop('relation')
    if relation not in RELATIONS:
        return
    schema, subject_prop, object_prop, prop = RELATIONS.get(relation)
    proxy = model.make_entity(schema)
    proxy.make_id(entity.pop('edge'))
    if prop is not None:
        proxy.add(prop, relation)
    subject = model.make_entity('Thing')
    subject.make_id(entity.pop('subject'))
    proxy.add(subject_prop, subject.id)
    object_ = model.make_entity('Thing')
    object_.make_id(entity.pop('object'))
    proxy.add(object_prop, object_.id)

    for section, values in entity.items():
        for value in values:
            if section == 'Source':
                # TODO: no 'proof' on Intervals.
                upload_document(root_path, documents, api, cid, value)
                continue
            prop = PROPERTIES.get(section)
            if schema == 'Associate' and prop == 'role':
                prop = 'relationship'
            proxy.add(prop, value)

    print(repr(proxy))
    yield proxy


def generate_entities(json_file, root_path, api, cid):
    with open(json_file, 'r') as fh:
        entities = json.load(fh)

    documents = {}
    for entity in entities:
        if not entity.get('id', '').startswith('documentos/'):
            continue
        # pprint(entity)
        for doc in entity.get('Documento', []):
            upload_document(root_path, documents, api, cid,
                            doc, title=entity.get('title'))

    for entity in entities:
        proxies = set()
        if 'id' in entity:
            proxies.update(make_node(root_path, documents, api, cid, entity))
        elif 'relation' in entity:
            res = make_relation(root_path, documents, api, cid, entity)
            proxies.update(res)
        for proxy in proxies:
            yield proxy.to_dict()


def load_entities(json_file, root_path):
    api = AlephAPI()
    collection = api.load_collection_by_foreign_id('zz_occrp_pdi')
    cid = collection.get('id')
    api.write_entities(cid, generate_entities(json_file, root_path, api, cid))


if __name__ == '__main__':
    load_entities('pdi.json', 'www.personadeinteres.org')
