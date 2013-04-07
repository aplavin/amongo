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

    def group(self, by=None, count=False):
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