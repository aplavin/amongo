from operator import getitem


class AMongoObject(object):
    def __init__(self, collection):
        self.collection = collection
        self.pipeline = []

    def execute(self):
        if self.pipeline:
            res = self.collection.aggregate(self.pipeline)
            if res['ok'] == 1:
                return res['result']
            else:
                raise Exception('MongoDb returned not OK')
        else:
            return self.collection.find()

    def group(self, by, count=False):
        group_stage = {}
        project_stage = {}

        group_stage['_id'] = {}

        if isinstance(by, basestring):
            by = (by, )

        for key_part in by:
            if isinstance(key_part, basestring):
                group_stage['_id'][key_part] = '$%s' % key_part
                project_stage[key_part] = '$_id.%s' % key_part
            else:
                raise Exception('Unsupported value in "by" parameter')

        if count is True:
            group_stage['count'] = {'$sum': 1}
            project_stage['count'] = True

        if '_id' not in project_stage:
            project_stage['_id'] = False

        self.pipeline.append({'$group': group_stage})
        self.pipeline.append({'$project': project_stage})
        return self

    def sort(self, **kwargs):
        presets = {True: 1, 'asc': 1, 'desc': -1}
        by = [(key, presets[d] if d in presets else d) for key, d in kwargs.items()]
        self.pipeline.append({'$sort': {key: d for key, d in by}})
        return self

    def limit(self, count):
        self.pipeline.append({'$limit': count})
        return self

    def top(self, number_of_items, **kwargs):
        return self.sort(**kwargs).limit(number_of_items)