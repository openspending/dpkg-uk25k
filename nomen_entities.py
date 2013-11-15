import sys
import argparse
import requests

from common import *
from running_stats import StatsList

nk_dataset_name = 'uk25k-entities'

# Model examples
# Entity
# {'_dataset': <Dataset(uk25k-entities)>, '__data__': {u'name': u'Therapies', u'creator': {u'updated_at': u'2012-07-18T13:20:27.593385', u'created_at': u'2012-07-18T13:20:27.593374', u'login': u'pudo', u'github_id': 41628, u'id': 1}, u'created_at': u'2012-07-30T12:19:35.227528', u'updated_at': u'2012-07-30T12:19:35.227540', u'dataset': [u'uk25k-entities'], u'data': [{}], u'id': 17920}}
# Alias (unmatched)
# {'_dataset': <Dataset(uk25k-entities)>, '__data__': {u'created_at': u'2012-09-29T23:02:43.650686', u'name': u'IPS - Identity & Passport Service ', u'creator': {u'updated_at': u'2012-07-18T13:20:27.593385', u'created_at': u'2012-07-18T13:20:27.593374', u'login': u'pudo', u'github_id': 41628, u'id': 1}, u'matcher': None, u'is_invalid': False, u'is_matched': False, u'updated_at': u'2012-09-29T23:02:43.650698', u'entity': None, u'dataset': u'uk25k-entities', u'data': {}, u'id': 25712}}

class NomenData(object):
    '''Data from Nomenklatura that has been processed and cached.'''
    def __init__(self):
        nk_dataset = nk_connect(nk_dataset_name)
        # Put all the entities and their aliases in entitie_dict, invalid_aliases and unmatched_aliases
        entities = nk_dataset.entities()
        self.entities_dict = {} # id: entity_dict
        self.entities_dict_by_name = {} # name: entity_dict
        count = 0
        for entity in entities:
            count += 1
            self.entities_dict[entity.id] = {
                    'entity': entity,
                    'has_dgu_name': bool(entity.data.get('dgu_name')),
                    'aliases': []}
            self.entities_dict_by_name[entity.name] = self.entities_dict[entity.id]
        self.num_entities = count

        count = 0
        aliases = nk_dataset.aliases()
        self.aliases_by_name = {} # name: alias
        self.invalid_aliases = []
        self.unmatched_aliases = []
        for al in aliases:
            count += 1
            self.aliases_by_name[al.name] = al
            if al.is_matched and not al.is_invalid:
                self.entities_dict[al.entity['id']]['aliases'].append(al)
                # self.entities_dict_by_name has the same value object, so no
                # need to update it separately.
            elif al.is_matched and al.is_invalid:
                self.invalid_aliases.append(al)
            else:
                self.unmatched_aliases.append(al)
        self.num_aliases = count

def show_all():
    nomen_data = NomenData()
    print 'Entities: %i' % nomen_data.num_entities
    print 'Aliases: %i' % nomen_data.num_aliases

    def title(text):
        return '\n\n\n%s\n%s\n%s' % ('*' * 70, text, '*' * 70)

    print title('Entity: Alias, Alias, Alias...')
    def printable_nkobj(alias_or_entity):
        # Use UTF8 (matching the average shell) so it can be cut/pasted
        return alias_or_entity.name.encode('utf8', errors='replace')
    for entity_dict in sorted(nomen_data.entities_dict.values(),
            key=lambda entity_dict: (entity_dict['has_dgu_name'],
                                     entity_dict['entity'].name)):
        entity = entity_dict['entity']
        aliases = entity_dict['aliases']
        print printable_nkobj(entity), \
                ' '.join(['"%s"' % printable_nkobj(alias) for alias in aliases])

    print title('Invalid Aliases')
    for alias in sorted(nomen_data.invalid_aliases,
            key=lambda alias: alias.name):
        print printable_nkobj(alias)

    print title('Unmatched Aliases')
    for alias in sorted(nomen_data.unmatched_aliases,
            key=lambda alias: alias.name):
        print printable_nkobj(alias)

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

def reconcile_aliases_that_match_entities_exactly():
    '''When adding entities using this tool, they might also currently be in
    the recon queue. In cases there the alias name matches exactly the entity
    name, link them up.

    (Ideally we'd just delete the alias from the recon queue, but there is no
    delete_alias API.)
    '''
    stats = StatsList()
    nomen_data = NomenData()
    nk_dataset = nk_connect(nk_dataset_name)
    for alias in nomen_data.unmatched_aliases:
        try:
            entity_or_alias = nk_dataset.lookup_detailed(alias.name, readonly=True)
        except (nk_dataset.Invalid, nk_dataset.NoMatch):
            entity_or_alias = None

        if entity_or_alias and isinstance(entity_or_alias, nomenklatura.Entity):
            try:
                nk_dataset.match(alias_id=alias.id, entity_id=entity_or_alias.id)
            except requests.exceptions.HTTPError, e:
                # Seem to get occasional 502s due to overloading
                print stats.add('Server error linking the alias to an entity: %s' % e, alias.name)
                continue
            print stats.add('Matched alias to an entity of the same name', alias.name)
        else:
            print stats.add('No matching entity', alias.name)
    print 'Summary'
    print stats.report()

def bulk_action(action=None, filepath=None, entity_or_alias_names=None, entities=True, aliases=True):
    nomen_data = NomenData()
    nk_dataset = nk_connect(nk_dataset_name)

    # Gather the list of entities & aliases from the file and command-line
    entities_or_aliases = []
    def find_name(name, stats):
        if not name.strip():
            print stats.add('blank', name)
        elif entities and name in nomen_data.entities_dict_by_name:
            entities_or_aliases.append(nomen_data.entities_dict_by_name[name]['entity'])
            print stats.add('Entity found', name)
        elif aliases and name in nomen_data.aliases_by_name:
            entities_or_aliases.append(nomen_data.aliases_by_name[name])
            print stats.add('Alias found', name)
        else:
            print stats.add('Not found', name)
    if entity_or_alias_names:
        stats = StatsList()
        for name in entity_or_alias_names:
            find_name(name, stats)
        print 'Given names:'
        print stats.report()
    if filepath:
        if not os.path.exists(filepath):
            raise Exception('Filepath not found: %s' % filepath)
        with open(filepath, 'r') as f:
            stats = StatsList()
            for line in f:
                name = line.rstrip('\n\r')
                find_name(name, stats)
                #try:
                #    entity_or_alias = nk_dataset.lookup_detailed(publisher['title'], readonly=True)
                #except nk_dataset.NoMatch:
                #    print stats.add('Not found', publisher['title'])
                #    continue
                #except nk_dataset.Invalid:
                #    pass
                #print stats.add('Found %s' % entity_or_alias.__class__.__name__, entity_or_alias.name)
                #entities_or_aliases.append(entity_or_alias)
        print 'File names:'
        print stats.report()

    # Do the action to each entity
    stats = StatsList()
    for entity_or_alias in entities_or_aliases:
        name = entity_or_alias.name
        if action=='invalidate':
            if isinstance(entity_or_alias, nomenklatura.Entity):
                print stats.add('Cannot invalidate an Entity', name)
                continue
            alias = entity_or_alias
            if alias.is_invalid:
                print stats.add('Already invalid', name)
                continue
            try:
                nk_dataset.match(alias_id=alias.id, entity_id='INVALID')
            except requests.exceptions.HTTPError, e:
                # Seem to get occasional 502s due to overloading
                print stats.add('Server error: %s' % e, alias.name)
                continue
            print stats.add('Invalidated', name)
        else:
            raise NotImplemented
    print 'Bulk %s:' % action
    print stats.report()

if __name__ == '__main__':
    parser1 = argparse.ArgumentParser(description='Manager of Entities in Nomenklatura.')
    commands = (
        'show',
        'update-entities-from-dgu',
        'reconcile-aliases-that-match-entities-exactly',
        'invalidate',
        )
    parser1.add_argument('command', choices=commands)

    # Split command-line into the command and any args after it
    # (there may be general options before the command)
    # TODO: use add_subparsers instead
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
    elif parsed_args1.command == 'reconcile-aliases-that-match-entities-exactly':
        reconcile_aliases_that_match_entities_exactly()
    elif parsed_args1.command == 'invalidate':
        parser2 = argparse.ArgumentParser()
        parser2.add_argument('-f', metavar='FILE', dest='filepath')
        parser2.add_argument('entities', nargs='*', default=None)
        parsed_args2 = parser2.parse_args(args2)
        bulk_action(action=parsed_args1.command, filepath=parsed_args2.filepath, entity_or_alias_names=parsed_args2.entities)
    else:
        raise NotImplemented

