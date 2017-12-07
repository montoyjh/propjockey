from operator import itemgetter
from functools import wraps

app = Flask(__name__)

app.config.from_envvar('PROPJOCKEY_SETTINGS', silent=True)

wconf = app.config['WORKFLOWS'] # Dunno if this is the best place for this, could be somewhere else

lpad = wconf['launchpad']

logger = logging.get_logger(__module__)

def set_priority(propjockey_field, fireworks_field, vote_weight=1,
                 pj_filter={}, base_priority=0, time_window=None, 
                 modifier_func=None):
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
        pj_filter (dict): filter for querying distinct keys to relate
            to new priorities
        base_priority (float): base priority for fws to be modified
        time_window (float): time in hours to limit db query to
        modifier_func (function): function that determines modifier
            for function based on propjockey document and firework.
            Should have two arguments propjockey_doc and firework,
            which take propjockey documents and fireworks as args.
            Example: want to penalize structures with large nsites
    """
    pj_filter.update({"state": {"$ne": "COMPLETED"}})
    if time_window:
        # TODO: fix this
        td = timedelta(hours=time_window)
        min_time = datetime.utcnow() - td
        pj_filter.update({"last_updated": {"$gt": min_time}})
    e_ids = pj_collection.distinct(propjockey_field, pj_filter)
    # TODO: should this be parallelized?
    # TODO: this should probably have a filter for stuff that's been updated
    #           recently so you don't have to reset every priority, but I
    #           think propjockey needs to be modified slightly for this.
    for e_id in e_ids:
        pj_doc = pj_collection.find_one({propjockey_field: e_id})
        fw_ids = lpad.get_fw_ids(query={fireworks_field: e_id})
        new_priority = vote_weight * pj_doc['nrequesters'] + base_priority
        for fw_id in fw_ids:
            if modifier_func:
                firework = lp.get_fw_by_id(fw_id)
                new_priority = modifier_func(propjockey_doc, firework)
            logger.info("Setting priority of fw with id {} to {}".format(fw_id, new_priority))
            lpad.set_priority(fw_id, new_priority)

if __name__ == "__main__":
    prioritizer_args = wconf['priority']
    set_priority(**prioritizer_args)
