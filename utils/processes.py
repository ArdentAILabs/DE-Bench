import os
import subprocess
from typing import List, Union


def run_and_validate_subprocess(
    command: List[str],
    process_description: str = "",
    check: bool = True,
    capture_output: bool = True,
    return_output: bool = False,
    input_text: str = None,
) -> Union[subprocess.CompletedProcess, str]:
    """
    Helper function to run a subprocess command and validate the return code.

    Args:
        command: The command to run
        process_description: Description of the process for error messages
        check: Whether to check the return code
        capture_output: Whether to capture the output
        return_output: Whether to return the output
        input_text: Text to send to stdin if command expects input

    Returns:
        The completed process or command output if return_output is True
    """
    try:
        if input_text:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = process.communicate(input=input_text)
            if process.returncode != 0:
                print(stderr)
                raise subprocess.CalledProcessError(
                    process.returncode, command, stdout, stderr
                )
            if return_output:
                return stdout
            else:
                return subprocess.CompletedProcess(
                    command, process.returncode, stdout, stderr
                )
        else:
            process = subprocess.run(
                command, check=check, capture_output=capture_output
            )
            if process.returncode != 0:
                stderr_text = process.stderr.decode("utf-8") if process.stderr else ""
                stdout_text = process.stdout.decode("utf-8") if process.stdout else ""
                print(stderr_text)
                raise subprocess.CalledProcessError(
                    process.returncode, command, stdout_text, stderr_text
                )
            if return_output:
                return process.stdout.decode("utf-8").rstrip("\n")
            else:
                return process
    except Exception as e:
        print(f"Worker {os.getpid()}: Error running {process_description}: {e}")
        raise e from e
