from prefect import flow, task
import random
import asyncio
from prefect.client.orchestration import get_client

@task
def process_data(team: str, run: int) -> bool:
    """Simulate data processing with failures"""
    
    # Simulate persistent failures
    if team == "b" and run > 2:
        raise Exception(f"Failure in {team} data processing")
    
    return True

@flow(name="data-pipeline")
def team_pipeline(team: str, run: int):
    """Main flow for data processing"""
    process_data(team, run)

async def create_runs(team_deployments):
    """Async function to create and space out the flow runs"""
    client = get_client()
    
    # Calculate time intervals to spread 20 runs over 1 minute
    total_duration = 60  # 1 minute in seconds
    interval = total_duration / 20  # Time between each run
    
    for i in range(20):
        team = random.choice(["a", "b"])
        deployment_id = team_deployments[team]["id"]
        runs = team_deployments[team]["runs"] + 1
        
        try:
            # Create and execute the run
            await client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters={"team": team, "run": runs}
            )
            team_deployments[team]["runs"] = runs
            print(f"Started run {runs} for team {team}")
        except Exception as e:
            print(f"Error starting run {runs} for team {team}: {str(e)}")
        
        # Sleep for the interval
        await asyncio.sleep(interval)

if __name__ == "__main__":
    # Deploy for Team A
    team_a_deployment_id = team_pipeline.deploy(
        name="team-a-pipeline",
        work_pool_name="team-a-pool",
        image="prefecthq/prefect:3-python3.9",
        push=False,
        parameters={"team": "a"},
        tags=["team-a", "yellow"]
    )

    # Deploy for Team B
    team_b_deployment_id = team_pipeline.deploy(
        name="team-b-pipeline",
        work_pool_name="team-b-pool",
        image="prefecthq/prefect:3-python3.9",
        push=False,
        parameters={"team": "b"},
        tags=["team-b", "green"]
    )

    # Store deployment IDs
    team_deployments = {
        "a": {
            "id": team_a_deployment_id,
            "runs": 0
        },
        "b": {
            "id": team_b_deployment_id,
            "runs": 0
        }
    }

    # Run the async function
    asyncio.run(create_runs(team_deployments))
