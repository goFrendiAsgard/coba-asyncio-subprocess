from typing import Mapping
import asyncio
import datetime
import sys
import logging

async def stream_reader(stream, queue):
    while True:
        line = await stream.readline()
        if not line:
            break
        await queue.put(line)

def get_line(process_name, pid: int, attempt: int, retry: int, line) -> str:
    max_attempt = attempt + retry
    content = line.decode()
    now = datetime.datetime.now().isoformat()
    return f'{now} PID={pid}, Attempt {attempt} of {max_attempt}, {process_name} {content}'

async def stream_stdout_logger(queue, process_name: str, pid: int, attempt: int, retry: int):
    while True:
        line = await queue.get()
        if not line:
            break
        print(get_line(process_name, pid, attempt, retry, line))

async def stream_stderr_logger(queue, process_name: str, pid: int, attempt: int, retry: int):
    while True:
        line = await queue.get()
        if not line:
            break
        print(get_line(process_name, pid, attempt, retry, line), file=sys.stderr)

async def run_command(
    command: str, process_name: str, attempt: int = 1, retry: int = 2, retry_delay: int = 1
):
    process = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    pid = process.pid
    # Create queue
    stdout_queue = asyncio.Queue()
    stderr_queue = asyncio.Queue()
    # Create reader task
    stdout_task = asyncio.create_task(stream_reader(
        process.stdout, stdout_queue
    ))
    stderr_task = asyncio.create_task(
        stream_reader(process.stderr, stderr_queue
    ))
    # Create logger task
    stdout_logger_task = asyncio.create_task(stream_stdout_logger(
        stdout_queue, process_name, pid, attempt, retry
    ))
    stderr_logger_task = asyncio.create_task(stream_stderr_logger(
        stderr_queue, process_name, pid, attempt, retry
    ))
    # wait process
    await process.wait()
    await stdout_queue.put(None)
    await stderr_queue.put(None)
    # wait reader and logger
    await stdout_task
    await stderr_task
    await stdout_logger_task
    await stderr_logger_task
    # get return code
    return_code = process.returncode
    if return_code == 0:
        return return_code
    if return_code != 0 and retry <= 0:
        raise Exception(f'Process {process_name} exited ({return_code})')
    logging.info(f'Retrying to run {process_name} in {retry_delay} second(s)')
    await asyncio.sleep(delay=retry_delay)
    return await run_command(
        command, process_name, attempt=attempt+1, retry=retry-1, retry_delay=retry_delay
    )


async def main(commands: Mapping[str, str]):
    tasks = []
    for process_name, command in commands.items():
        tasks.append(asyncio.create_task(
            run_command(command, process_name, attempt=1, retry=2)
        ))
    result = await asyncio.gather(*tasks)
    print(result)

commands = {
    'process1': './ding.sh',
    'process2': './dong.sh',
    'process3': 'cowsay aduh',
    # 'process4': 'python -m http.server',
    'process4': 'ulalal',
    'process5': 'sleep 3 && exit 1'
}


asyncio.run(main(commands))