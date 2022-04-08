from typing import Dict, List, Optional, Literal, Union
from pydantic import BaseModel, validator
from datetime import datetime

# datetime.strptime("2022-01-01T04:16:13+10:00", "%Y-%m-%dT%H:%M:%S%z")

# CONSTs or as close to them as Python gets
required_keys = {
    "symbol",
    "date_from",
    "date_to",
    "ta_algos",
    "resolution",
    "search_period",
    "target_ta_confidence",
}

optional_keys = {
    "notify_method",
    "notify_recipient",
}

valid_notify_methods = [None, "pushover", "slack"]

# for the purposes of validation, create objects representing the incoming data structure
# then use Pydantic to validate them
# dunno if this is overkill but yolo
class Algo(BaseModel):
    # i don't know if there are any attributes that will be mandatory across all algos
    # but just in case i guess
    ...


class AwesomeOscillator(Algo):
    strategy: Optional[Literal["saucer", "twin-peaks", "crossover"]] = None
    direction: Optional[Literal["bullish", "bearish"]] = None


# todo
class Stoch(Algo):
    ...


# todo
class AccumulationDistribution(Algo):
    ...


class Job(BaseModel):
    symbol: str
    date_from: datetime
    date_to: datetime
    ta_algos: List[Union[AwesomeOscillator, Stoch, AccumulationDistribution]]
    resolution: str
    search_period: int
    notify_method: Optional[str]
    notify_recipient: Optional[str]
    target_ta_confidence: int

    @validator("notify_method")
    def notify_checker(cls, v):
        if not v in valid_notify_methods:
            raise ValueError(
                f"Invalid notification_method specified {v}. Must be one of {str(valid_notify_methods)}"
            )

    @validator("resolution")
    def resolution_checker(cls, v):
        valid_resolutions = [
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
        ]
        if v not in valid_resolutions:
            raise ValueError(
                f"Invalid resolution specified: {v}. Must be one of {str(valid_resolutions)}"
            )

    @validator("target_ta_confidence")
    def target_ta_confidence_checker(cls, v):
        if v < 0 or v > 10:
            raise ValueError(
                f"Invalid target_ta_confidence specified: {v}. Must be between 0 and 10"
            )


# Function to flatten a list of jobs that contains a list of symbols and a list of TA algorithms
# Enumerate the permutations of job x symbol x algo to come up with a list/array that is fed
# into the Step Function map
def job_enumerator(job_object):
    jobs = []
    # enumerate jobs
    for job in job_object["jobs"]:
        # enumerate the algos in this job
        for algo in job["ta_algos"]:
            # take a copy of the parent job, remove the lists and add the specific list item we want in this job
            # i've done this so that the job data structure can change without me needing to update the rest of the pipeline
            this_job = job.copy()
            del this_job["ta_algos"]
            this_job["ta_algo"] = algo
            jobs.append(this_job)

        # even though ive set this up to take multiple jobs, the rest of the flow is not ready for that yet
        # so stop it after one job
        break

    return jobs


# checks that the mandatory keys are specified in the query
# also looks for additional keys that are not implemented in the query
def check_required_keys(jobs):
    # check keys
    for job in jobs:
        present_keys = set(job.keys())

        missing_keys = required_keys - present_keys
        unrecognised_keys = present_keys - required_keys
        unrecognised_keys = unrecognised_keys - optional_keys

        if len(missing_keys) > 0:
            # at least one key was not provided
            raise KeyError(
                f"One or more keys were missing from your request: {str(missing_keys)}"
            )

        if len(unrecognised_keys) > 0:
            raise KeyError(
                f"One or more keys provided in your request are unknown: {str(unrecognised_keys)}"
            )

    return True


# i hate this code
# basically checks that the algos data structure matches what i expect - types and literal algos
def check_algos(algos):
    # in case there's nothing
    if algos == None:
        raise KeyError(f"A TA algorithm was not specified")

    # in case what's given is not a list
    if isinstance(algos, list) != True:
        raise TypeError(
            f"TA algorithms must be specified as a list array.  Instead found {str(type(algos))}"
        )

    algo_objects = []

    for algo in algos:
        if isinstance(algo, dict) != True:
            raise TypeError(
                f"TA algorithm definition must be specified as a dict with the algo as key and parameters in a nested dict.  Instead found {str(type(algo))}"
            )

        algo_name = list(algo.keys())[0]
        if algo_name == "awesome-oscillator":
            algo_objects.append(AwesomeOscillator())
        elif algo_name == "stoch":
            algo_objects.append(Stoch())
        elif algo_name == "accumulation-distribution":
            algo_objects.append(AccumulationDistribution())
        else:
            raise KeyError(f"Selected TA algorithm is not recognised: {algo_name}")

    return algo_objects


def add_optional_keys(job):
    present_keys = set(job.keys())
    keys_to_add = optional_keys - present_keys
    for key in keys_to_add:
        job[key] = None
    return job


# run this once we're sure these keys exist
# running this means we're applying our Pydantic validation to the query
def check_types(jobs):
    for job in jobs:
        # temporarily add optional keys so that we don't get key errors when we try run them through Pydantic below
        this_job = add_optional_keys(job)
        Job(
            symbol=this_job["symbol"],
            date_from=this_job["date_from"],
            date_to=this_job["date_to"],
            ta_algos=check_algos(this_job["ta_algos"]),
            resolution=this_job["resolution"],
            search_period=this_job["search_period"],
            notify_method=this_job["notify_method"],
            notify_recipient=this_job["notify_recipient"],
            target_ta_confidence=this_job["target_ta_confidence"],
        )
    return True


def validate_input(jobs):
    check_required_keys(jobs)
    check_types(jobs)
    return


def lambda_handler(event, context):
    jobs = event["Payload"]
    # event["Payload"] will need to include the json below - just using a hardcoded mock for now
    # even though this json is a list of jobs, in reality there should only be a single object in this array
    # this is because otherwise i need to move target_ta_confidence in to ta_algos and then find some way
    # to use it later in the flow
    # i may still do this in future but not yet

    # validate the input parameters
    validate_input(jobs["jobs"])

    # flatten the jobs and move on
    flat_jobs = job_enumerator(jobs)

    return flat_jobs


if __name__ == "__main__":
    jobs = {
        "jobs": [
            {
                "bana": "ana",
                "symbol": "btc-aud",
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "ta_algos": [
                    {
                        "awesome-oscillator": {
                            "strategy": "saucer",
                            "direction": "bullish",
                        }
                    },
                    {"stoch": None},
                    {"accumulation-distribution": None},
                ],
                "resolution": "1d",
                "search_period": 20,
                # "notify_method": "pushover",
                # "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 7,
            }
        ]
    }
    lambda_handler({"Payload": jobs}, "")
