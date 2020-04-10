import os
import json
from glob import glob
from lxml import html
from pprint import pprint  # noqa

SECTIONS = ('personas', 'empresas', 'propiedades', 'acusaciones', 'documentos')


def join_path(source_file, rel_file):
    dir_name = os.path.dirname(source_file)
    rel_file = os.path.join(dir_name, rel_file)
    return os.path.realpath(rel_file)


def parse_path(root_path, path):
    return os.path.relpath(path, root_path)


def parse_entities(root_path):
    entities = []
    root_path = os.path.abspath(root_path)
    for res in SECTIONS:
        prefix = os.path.join(root_path, res)
        # print(prefix)
        for path in glob('%s/[0-9]*.html' % prefix):
            if 'index' in path:
                continue
            for entity in parse_entity(root_path, path):
                if entity is not None:
                    # pprint(entity)
                    entities.append(entity)
    print('Entities: %s' % len(entities))
    with open('pdi.json', 'w') as fh:
        fh.write(json.dumps(entities, indent=2))


def parse_entity(root_path, path):
    try:
        doc = html.parse(path)
    except OSError:
        return

    data = {
        'id': parse_path(root_path, path),
        'title': doc.findtext('.//h1[@class="main-title"]'),
        'abstract': doc.findtext('.//*[@class="main-abstract"]/div'),
        'body': doc.findtext('.//*[@class="object-body"]/div')
    }
    for (section, value) in parse_properties(root_path, path, doc):
        if section not in data:
            data[section] = []
        data[section].append(value)

    yield data
    yield from parse_relations(root_path, path, doc)


def parse_properties(root_path, path, doc):
    box = doc.find('.//*[@class="object-properties node-properties"]')
    if box is None:
        box = doc.find('.//*[@class="object-properties edge-properties"]')
    if box is None:
        return
    section = None
    for definition in box:
        if definition.tag == 'dt':
            section = definition.text.replace(':', '').strip()
            continue
        values = definition.getchildren()
        if 'multivalue' in definition.get('class'):
            values = definition.findall('.//li')
        for value in values:
            link = value if value.tag == 'a' else value.find('./a')
            if link is not None:
                file_path = join_path(path, link.get('href'))
                file_path = os.path.relpath(file_path, root_path)
                yield (section, (link.text, file_path))
            elif value.get('datetime'):
                yield (section, value.get('datetime'))
            else:
                yield (section, value.text)


def parse_relation(root_path, path, relation, el):
    object_link = el.find('a[@class="object-link"]').get('href')
    object_link = join_path(path, object_link)
    edge_link = el.find('a[@class="edge-link"]').get('href')
    edge_link = join_path(path, edge_link)
    data = {
        'relation': relation,
        'subject': parse_path(root_path, path),
        'object': parse_path(root_path, object_link),
        'edge': os.path.relpath(edge_link, root_path)
    }
    try:
        doc = html.parse(edge_link)
    except OSError:
        # print("Path not found: %s" % edge_link)
        return data

    for (section, value) in parse_properties(root_path, edge_link, doc):
        data.setdefault(section, [])
        data[section].append(value)
    return data


def parse_relations(root_path, path, doc):
    for section in doc.findall('.//*[@class="edge-schema"]'):
        relation = section.findtext('.//h2')
        for el in section.findall('.//li'):
            yield parse_relation(root_path, path, relation, el)


if __name__ == '__main__':
    parse_entities('www.personadeinteres.org')
