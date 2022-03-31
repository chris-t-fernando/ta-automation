# Function to flatten a list of jobs that contains a list of symbols and a list of TA algorithms
# Enumerate the permutations of job x symbol x algo to come up with a list/array that is fed
# into the Step Function map
def job_enumerator(job_object):
    jobs = []
    # enumerate jobs
    for job in job_object["jobs"]:
        # enumerate the symbols in this job
        for symbol in job["symbols"]:
            # enumerate the algos in this job
            for algo in job["ta_algos"]:
                # take a copy of the parent job, remove the lists and add the specific list item we want in this job
                # i've done this so that the job data structure can change without me needing to update the rest of the pipeline
                this_job = job.copy()
                del this_job["ta_algos"]
                del this_job["symbols"]
                this_job["symbol"] = symbol
                this_job["ta_algo"] = algo
                jobs.append(this_job)

    return jobs


def lambda_handler(event, context):
    jobs = event["Payload"]
    # event["Payload"] will need to include the json below - just using a hardcoded mock for now
    # even though this json is a list of jobs, in reality there should only be a single object in this array
    # this is because otherwise i need to move target_ta_confidence in to ta_algos and then find some way
    # to use it later in the flow
    # i may still do this in future but not yet
    jobs = {
        "jobs": [
            {
                "symbols": ["bhp", "tls", "nea"],
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-01-14T04:16:13+10:00",
                "ta_algos": ["awesome-oscillator", "stoch", "accelerator-oscillator"],
                "resolution": "1d",
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 7.5,
            }
        ]
    }

    flat_jobs = job_enumerator(jobs)

    return flat_jobs


if __name__ == "__main__":
    lambda_handler("", "")
