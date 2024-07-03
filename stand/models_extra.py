## duplicated model that also exists in tahiti
## this class should be used only for convenience
class PipelineStep():
    """ Pipeline step """

    def __init__(self, id=None, name=None, order=None,
                 scheduling=None, trigger_type=None,description=None, enabled=None,
                 workflow_type=None, workflow_id=None):
        self.id = id
        self.name = name
        self.order = order
        self.scheduling = scheduling
        self.trigger_type =trigger_type
        self.description = description
        self.enabled = enabled
        self.workflow_type = workflow_type
        self.workflow_id = workflow_id


    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)