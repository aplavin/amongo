

class AMongoObject(object):
    def __init__(self, collection):
        self.collection = collection
        self.pipeline = []

    def execute(self):
        if self.pipeline:
            return self.collection.aggregate(self.pipeline)
        else:
            return self.collection.find()

    def group(self, **kwargs):
        group_stage = {}
        project_stage = {}

        group_stage['_id'] = {}
        for key_part in kwargs['by']:
            if isinstance(key_part, basestring):
                group_stage['_id'][key_part] = '$%s' % key_part
                project_stage[key_part] = '$_id.%s' % key_part

        self.pipeline.append({'$group': group_stage})
        self.pipeline.append({'$project': project_stage})
        return self