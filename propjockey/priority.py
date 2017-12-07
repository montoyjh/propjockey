from operator import itemgetter
from functools import wraps

app = Flask(__name__)

app.config.from_envvar('PROPJOCKEY_SETTINGS', silent=True)

wconf = app.config['WORKFLOWS']

lpad = LaunchPad()#TODO: figure out way to set)

logger = logging.get

def set_priority(propjockey_field, fireworks_field, vote_weight=1, 
                 pj_filter={}, base_priority=0, modifier_func=None):
    """
    Function to set the priority of fireworks managed by propjockey.

    Priorities are calculated by:
        priority = base_priority + vote_weight * NUM_VOTES + modifier_func

    Args:
        propjockey_field (mongolike key): Field for which the
            value can be related to fireworks data, e.g. material_id
        fireworks_field (mongolike key): Field for the corresponding
            firework should be matched to the propjockey_field, e.g. 
            spec.tags or spec.snl.about._mp_id.
        vote_weight (float): weight assigned to votes in calculating
            new fw priority
        base_priority (float): base priority for fws to be modified
        modifier_func (function): function that determines modifier
            for function based on propjockey document and firework.
            Should have two arguments propjockey_doc and firework,
            which take propjockey documents and fireworks as args.
    """
    pj_filter = pj_filter.update({{"state": {"$ne": "COMPLETED"}}
    e_ids = pj_collection.distinct(propjockey_field, pj_filter)
    # TODO: should this be parallelized?
    # TODO: this should probably have a filter for stuff that's been updated
    #           recently so you don't have to reset every priority, but I
    #           think propjockey needs to be modified slightly for this.
    for e_id in e_ids:
        propjockey_doc = pj_collection.find_one({propjockey_field: e_id})
        fw_id = lpad.get_fw_ids(query={fireworks_field: e_id})
        new_priority = base_priority + propjockey_doc['nrequesters'] * vote_weight
        if modifier_func:
            firework = lpad.get_fw_by_id(fw_id)
            new_priority += modifier_func(propjockey_doc, firework)
        logger.info("Updating fw_id {} with priority {}".format(fw_id, new_priority))
        lpad.set_priority(fw_id, new_priority)
