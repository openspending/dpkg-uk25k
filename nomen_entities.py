import sys
import argparse

from common import *
from running_stats import StatsList

nk_dataset_name = 'uk25k-entities'

# Model examples
# Entity
# {'_dataset': <Dataset(uk25k-entities)>, '__data__': {u'name': u'Therapies', u'creator': {u'updated_at': u'2012-07-18T13:20:27.593385', u'created_at': u'2012-07-18T13:20:27.593374', u'login': u'pudo', u'github_id': 41628, u'id': 1}, u'created_at': u'2012-07-30T12:19:35.227528', u'updated_at': u'2012-07-30T12:19:35.227540', u'dataset': [u'uk25k-entities'], u'data': [{}], u'id': 17920}}
# Alias (unmatched)
# {'_dataset': <Dataset(uk25k-entities)>, '__data__': {u'created_at': u'2012-09-29T23:02:43.650686', u'name': u'IPS - Identity & Passport Service ', u'creator': {u'updated_at': u'2012-07-18T13:20:27.593385', u'created_at': u'2012-07-18T13:20:27.593374', u'login': u'pudo', u'github_id': 41628, u'id': 1}, u'matcher': None, u'is_invalid': False, u'is_matched': False, u'updated_at': u'2012-09-29T23:02:43.650698', u'entity': None, u'dataset': u'uk25k-entities', u'data': {}, u'id': 25712}}

def show_all():
    nk_dataset = nk_connect(nk_dataset_name)
    entities = nk_dataset.entities()
    for entity in entities:
        print entity
    aliases = nk_dataset.aliases()
    for al in aliases:
        print al

def update_entities_from_dgu(publishers=None):
    dgu_client = ckan_client()
    if not publishers:
        # Get list of publishers from DGU
        publishers = dgu_client.action('organization_list')

    stats = StatsList()
    nk_dataset = nk_connect(nk_dataset_name)
    for publisher_name in publishers:
        publisher = dgu_client.action('organization_show', id=publisher_name)

        # Match each publisher with a Nomen entity
        try:
            entity_or_alias = nk_dataset.lookup_detailed(publisher['title'], readonly=True)
        except (nk_dataset.Invalid, nk_dataset.NoMatch):
            entity_or_alias = None

        data = {'dgu_name': publisher_name}
        if entity_or_alias and isinstance(entity_or_alias, nomenklatura.Entity):
            # Matched an entity
            entity = entity_or_alias
            if entity.data.get('dgu_name') == publisher_name:
                # Matching ID, ensure Nomen still has the title as per DGU
                print stats.add('Matching ID. Title match: %s' % \
                        (entity.name == publisher['title']), publisher_name)
            elif 'dgu_name' in entity.data:
                print stats.add('Wrong ID - ignoring', publisher_name)
            elif entity.name == publisher['title']:
                nk_dataset.update_entity(entity.id, entity.name, data)
                print stats.add('Matching title, just added ID', publisher_name)
            else:
                # The title differs because of canonization? Hasn't happened yet.
                print stats.add('Title differs - ignoring', publisher_name)
        elif entity_or_alias and isinstance(entity_or_alias, nomenklatura.Alias):
            # Matched an alias
            alias_ = entity_or_alias
            if alias_.is_matched:
                entity = nk_dataset.get_entity(id=alias_.entity['id'])
                if entity.data.get('dgu_name'):
                    print stats.add('Matched an alias for an entity which already has an ID - ignoring', publisher_name)
                else:
                    nk_dataset.update_entity(entity.id, publisher['title'], data)
                    # we can't delete the existing alias (that is now the same
                    # as the entity) but we can create a new alias for the old
                    # entity
                    try:
                        new_alias = nk_dataset.lookup(entity.name)
                    except nk_dataset.NoMatch:
                        nk_dataset.match(alias_id=new_alias.id, entity_id=entity.id)
                        print stats.add('Matched an alias for an entity - swapped them over', publisher_name)
                    except nk_dataset.Invalid:
                        # This is not expected, but still fine
                        print stats.add('Matched an alias for an entity - overwrote the entity', publisher_name)
                    else:
                        # This is not expected, but still fine
                        print stats.add('Matched an alias for an entity - overwrote the entity', publisher_name)
            else:
                new_entity = nk_dataset.add_entity(publisher['title'], data)
                nk_dataset.match(alias_id=alias_.id, entity_id=new_entity.id)
                print stats.add('Matched an alias without a matching entity - created the entity')
        else:
            # No match - create Nomen entity
            nk_dataset.add_entity(publisher['title'], data)
            print stats.add('No match - added to Nomen', publisher_name)
    print 'Summary'
    print stats.report()

if __name__ == '__main__':
    parser1 = argparse.ArgumentParser(description='Manager of Entities in Nomenklatura.')
    commands = ('show', 'update-entities-from-dgu')
    parser1.add_argument('command', choices=commands)

    # Split command-line into the command and any args after it
    # (there may be general options before the command)
    args1, args2 = sys.argv, []
    args = sys.argv[1:]
    for word_index, word in enumerate(args):
        if word in commands:
            args1, args2 = args[:word_index+1], args[word_index+1:]
            break
    parsed_args1 = parser1.parse_args(args1)
    if parsed_args1.command == 'show':
        show_all()
    elif parsed_args1.command == 'update-entities-from-dgu':
        parser2 = argparse.ArgumentParser()
        parser2.add_argument('publishers', nargs='*', default=None)
        parsed_args2 = parser2.parse_args(args2)
        update_entities_from_dgu(parsed_args2.publishers)
