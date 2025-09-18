"""
This module provides a class for managing GitHub operations.
"""

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import github
from github import Github, Repository


class GitHubManager:
    """
    A class to manage GitHub operations for testing.
    Handles repository setup, branch management, PR operations, and cleanup.
    """
    
    def __init__(self, access_token: str, repo_url: str, test_name: str, create_branch: bool = True, build_info: Optional[Dict[str, str]] = None):
        """
        Initialize the GitHub manager.
        
        :param access_token: GitHub access token
        :param repo_url: GitHub repository URL
        """
        self.access_token = access_token
        self.repo_url = repo_url
        self.repo_name = self._parse_repo_name(repo_url)
        self.github_client = Github(access_token)
        self.repo: Repository = self.github_client.get_repo(self.repo_name)
        self.build_info = "./build-info.properties"
        self.create_branch = create_branch
        if create_branch:
            self.branch_name = self.create_test_branch(
                test_name=test_name,
                build_info=build_info
            )
        else:
            self.branch_name = test_name

    def create_test_branch(self, test_name: str, build_info: Optional[Dict[str, str]]) -> str:
        """
        Create a new branch for the test.

        :param str test_name: Name of the test
        :param Optional[Dict[str, str]] build_info: Build info dictionary to update
        :return: Name of the new branch
        :rtype: str
        """
        try:
            commit_sha = self.repo.get_commits()[0].sha
            self.repo.create_git_ref(ref=f"refs/heads/{test_name}", sha=commit_sha)
            self.branch_name = test_name
            print(f"✓ Created branch: {self.branch_name}")
        except Exception as e:
            if getattr(e, "status", None) == 422:
                print(f"Branch '{test_name}' already exists, skipping creation.")
                return getattr(self, "branch_name", test_name)
            raise Exception(f"✗ Error creating branch: {e}")
        finally:
            if build_info:
                self._update_build_info(build_info, test_name)
        return self.branch_name

    def add_merge_step_to_user_input(self, user_input: str) -> str:
        """
        Add merge step to the user input string.

        :param str user_input: User input string to modify
        :return: Modified user input string with merge step
        :rtype: str
        """
        import re
        numbers = [int(n) for n in re.findall(r'\d+', user_input)]
        last_number = max(numbers) if numbers else 0
        # add the test name to the user input
        user_input += f"{last_number + 1}. Set the destination branch to '{self.branch_name}'."
        return user_input

    @staticmethod
    def _parse_repo_name(repo_url: str) -> str:
        """
        Parse repository name from URL.
        
        :param str repo_url: Full GitHub repository URL
        :return: Repository name in owner/repo format
        :rtype: str
        """
        if "github.com" in repo_url:
            parts = repo_url.split("/")
            return f"{parts[-2]}/{parts[-1]}"
        return repo_url

    def _iterate_directory_and_files(self, folder_name: str, keep_file_names: list[str]) -> None:
        for keep_file_name in keep_file_names:
            try:
                self.repo.get_contents(f"{folder_name}/{keep_file_name}")
                print(f"{folder_name}/{keep_file_name} already exists")
            except github.GithubException as e:
                if e.status == 404:
                    self.repo.create_file(
                        path=f"{folder_name}/{keep_file_name}",
                        message=f"Add {keep_file_name} to {folder_name} folder",
                        content="",
                        branch="main",
                    )
                    print(f"Created {folder_name}/{keep_file_name}")
                else:
                    raise e
    
    def get_file_content(self, file_path: str, branch: str = "main") -> Optional[str]:
        """
        Get the content of a file from the repository.
        
        :param str file_path: Path to the file in the repository
        :param str branch: Branch to get the file from (default: main)
        :return: File content as string, or None if file doesn't exist
        :rtype: Optional[str]
        """
        try:
            file_content = self.repo.get_contents(file_path, ref=branch)
            if isinstance(file_content, list):
                # If it's a directory, return None
                return None
            # Decode the content from base64
            return file_content.decoded_content.decode('utf-8')
        except github.GithubException as e:
            if e.status == 404:
                print(f"📄 File not found: {file_path}")
                return None
            else:
                print(f"❌ Error getting file content for {file_path}: {e}")
                raise e

    def get_all_dag_files(self, branch: str = None) -> dict:
        """
        Get all DAG files from the dags folder.
        
        :param str branch: Branch to get files from (default: uses current branch or main)
        :return: Dictionary mapping filename to content
        :rtype: dict
        """
        dag_files = {}
        
        # Use current branch if no branch specified
        if branch is None:
            branch = getattr(self, 'branch_name', 'main')
        
        print(f"🔍 Looking for DAG files in branch: {branch}")
        
        # Try current branch first, then main as fallback
        branches_to_try = [branch] if branch != 'main' else ['main']
        if branch != 'main':
            branches_to_try.append('main')
            
        for try_branch in branches_to_try:
            try:
                print(f"📁 Checking dags folder in branch: {try_branch}")
                # Get contents of dags folder
                dags_contents = self.repo.get_contents("dags", ref=try_branch)
                
                for content in dags_contents:
                    if content.type == "file" and content.name.endswith('.py'):
                        file_content = self.get_file_content(content.path, try_branch)
                        if file_content:
                            dag_files[content.name] = {
                                "path": content.path,
                                "content": file_content,
                                "size": content.size,
                                "sha": content.sha,
                                "branch": try_branch
                            }
                            print(f"✅ Found DAG file: {content.name} ({content.size} bytes)")
                
                # If we found files, break out of the loop
                if dag_files:
                    print(f"📄 Retrieved {len(dag_files)} DAG files from branch: {try_branch}")
                    break
                    
            except github.GithubException as e:
                if e.status == 404:
                    print(f"📁 Dags folder not found in branch: {try_branch}")
                else:
                    print(f"❌ Error getting DAG files from {try_branch}: {e}")
                continue
                
        return dag_files

    def clear_folder(self, folder_name: str, keep_file_names: Optional[list[str]] = None) -> None:
        """
        Clear the folder and ensure .gitkeep exists and nothing else.

        :param str folder_name: Name of the folder to setup
        :param Optional[list[str]] keep_file_names: List of file names to keep in the folder, defaults to [".gitkeep"]
        :rtype: None
        """
        if keep_file_names is None:
            keep_file_names = [".gitkeep"]
        try:
            # Clear folder contents except .gitkeep
            folder_contents = self.repo.get_contents(folder_name)
            for content in folder_contents:
                if content.name not in keep_file_names:
                    self.repo.delete_file(
                        path=content.path,
                        message=f"Clear {folder_name} folder",
                        sha=content.sha,
                        branch="main",
                    )
                    print(f"Deleted file: {content.path}")
            self._iterate_directory_and_files(folder_name, keep_file_names)

        except github.GithubException as e:
            if e.status == 404:
                # Folder doesn't exist, create it with .gitkeep
                print(f"Folder '{folder_name}' doesn't exist, creating it...")
                self._iterate_directory_and_files(folder_name, keep_file_names)
            elif "sha" not in str(e):  # If error is not about folder already existing
                raise e
            else:
                print(f"{folder_name} folder setup completed with warning: {e}")
    
    def verify_branch_exists(self, branch_name: str, test_step: Dict[str, str]) -> Tuple[bool, Dict[str, str]]:
        """
        Verify that a branch exists and update test steps.
        
        :param str branch_name: Name of the branch to check
        :param Dict[str, str] test_step: Test step dictionary
        :return: True if branch exists, False otherwise, and the updated test step
        :rtype: Tuple[bool, Dict[str, str]]
        """
        print(f"Checking if branch '{branch_name}' exists...")
        
        # List all branches for debugging
        try:
            branches = self.repo.get_branches()
            print(f"Found {branches.totalCount} branches in the {self.repo_name} repository.")
            branch_exists = any(branch.name == branch_name for branch in branches)
            print(f"{branch_exists=}")
        except Exception as e:
            raise Exception(f"Error listing branches: {e}")
        
        if branch_exists:
            test_step["status"] = "passed"
            test_step["Result_Message"] = f"Branch '{branch_name}' was created successfully"
            print(f"✓ Branch '{branch_name}' exists")
        else:
            test_step["status"] = "failed"
            test_step["Result_Message"] = f"Branch '{branch_name}' was not created."
            print(f"✗ Branch '{branch_name}' was not created.")
        return branch_exists, test_step
    
    def find_and_merge_pr(
            self,
            pr_title: str,
            test_step: Dict[str, str],
            commit_title: Optional[str] = None,
            merge_method: str = "squash",
            build_info: Optional[Dict[str, str]] = None,
            max_retries: Optional[int] = 10,
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Find a PR by title and merge it, updating test steps.
        
        :param str pr_title: Title of the PR to find
        :param Dict[str, str] test_step: Test step dictionary
        :param str commit_title: Custom commit title for merge (optional)
        :param str merge_method: Merge method ('squash', 'merge', 'rebase')
        :param Dict[str, str] build_info: Build info dictionary to update (optional)
        :param int max_retries: Maximum number of retries to find the PR, defaults to 10
        :return: True if PR was found and merged, False otherwise, and the updated test step
        :rtype: Tuple[bool, Dict[str, str]]
        """
        pulls = self.repo.get_pulls(state="open")
        target_pr = None
        
        print(f"Searching for PR with title: '{pr_title}'")
        print(f"Found {pulls.totalCount} open PRs:")
        for pr in pulls:
            print(f"  - PR: '{pr.title}' (branch: {pr.head.ref})")
            if pr.title == pr_title:
                target_pr = pr
                test_step["status"] = "passed"
                test_step["Result_Message"] = f"PR '{pr_title}' was created successfully"
                print(f"✓ Found PR: {pr_title}")
                break
        
        if not target_pr and max_retries > 0:
            print(f"✗ PR '{pr_title}' not found, retrying... ({max_retries} retries left)")
            time.sleep(5)
            # Return the result of the recursive retry so the caller gets the correct status
            return self.find_and_merge_pr(
                pr_title=pr_title,
                test_step=test_step,
                commit_title=commit_title,
                merge_method=merge_method,
                build_info=build_info,
                max_retries=max_retries - 1,
            )
        elif not target_pr and max_retries <= 0:
            test_step["status"] = "failed"
            test_step["Result_Message"] = f"PR '{pr_title}' not found"
            print(f"✗ PR '{pr_title}' not found")
            return False, test_step
        
        # Merge the PR with retry logic for handling conflicts in parallel execution
        merge_retries = 5  # Number of merge retries for conflicts
        for merge_attempt in range(merge_retries):
            try:
                if build_info:
                    # Get the branch name from the target PR
                    branch_name = target_pr.head.ref
                    self._update_build_info(build_info, branch_name)
                
                print(f"Attempting to merge PR '{pr_title}' (attempt {merge_attempt + 1}/{merge_retries})")
                merge_result = target_pr.merge(
                    commit_title=commit_title or pr_title,
                    merge_method=merge_method
                )
                
                if not merge_result.merged:
                    raise Exception(f"Merge failed: {merge_result.message}")
                
                print(f"✓ Successfully merged PR: {pr_title}")
                return True, test_step
                
            except Exception as e:
                error_message = str(e).lower()
                is_conflict = any(keyword in error_message for keyword in [
                    "base branch was modified", "merge conflict", "conflict", 
                    "405", "method not allowed", "review and try the merge again"
                ])
                
                if is_conflict and merge_attempt < merge_retries - 1:
                    # Wait with exponential backoff and jitter for parallel execution conflicts
                    import random
                    wait_time = (2 ** merge_attempt) + random.uniform(1, 3)  # 1-3s, 3-5s, 5-7s, etc.
                    print(f"⚠️ Merge conflict detected (attempt {merge_attempt + 1}): {e}")
                    print(f"⏳ Waiting {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)
                    
                    # Refresh the PR object to get latest state
                    try:
                        target_pr = self.repo.get_pull(target_pr.number)
                        print(f"🔄 Refreshed PR state for retry")
                    except Exception as refresh_error:
                        print(f"⚠️ Could not refresh PR state: {refresh_error}")
                    
                    continue  # Retry the merge
                else:
                    # Non-conflict error or max retries reached
                    if is_conflict:
                        print(f"❌ Failed to merge PR after {merge_retries} attempts due to persistent conflicts: {e}")
                    else:
                        print(f"❌ Failed to merge PR due to non-conflict error: {e}")
                    return False, test_step
        
        # Should not reach here, but just in case
        print(f"❌ Failed to merge PR after all retry attempts")
        return False, test_step

    def _update_build_info(self, build_info: Dict[str, str], branch_name: str) -> dict[str, Any]:
        """
        Update the build_info.txt file with the provided build info dictionary.
        Verifies the change was committed before returning.
        
        :param Dict[str, str] build_info: Build info dictionary
        :param str branch_name: Name of the branch to update the file on
        :return: The result of the update or create operation
        :rtype: dict[str, Any]
        """
        # use github api to update the build_info.txt file
        build_info_txt = ""
        for key, value in build_info.items():
            build_info_txt += f"{key.replace(' ', '_')}={value}\n"
        build_info_txt = build_info_txt.strip()
        
        result = None
        try:
            contents = self.repo.get_contents(self.build_info, ref=branch_name)
            # check if the file exists
            result = self.repo.update_file(
                path=self.build_info,
                message=f"Updated {self.build_info}",
                content=build_info_txt,
                branch=branch_name,
                sha=contents.sha,
            )
            print(f"✓ Build info updated successfully for branch {branch_name}")
            print(f"Build info: {build_info_txt}")
        except Exception as e:
            if e.status == 404:
                try:
                    result = self.repo.create_file(
                        path=self.build_info,
                        message=f"Created {self.build_info}",
                        content=build_info_txt,
                        branch=branch_name,
                    )
                    print(f"✓ Build info created successfully for branch {branch_name}")
                except Exception as e:
                    raise Exception(f"✗ Error creating build info for branch {branch_name}: {e}")
            else:
                raise Exception(f"Error updating build info: {e}")
        
        # Verify the change was committed by checking the new commit exists
        if result and 'commit' in result:
            commit_sha = result['commit'].sha
            try:
                # Verify the commit exists and is accessible
                _ = self.repo.get_commit(commit_sha)
                print(f"✓ Build info commit verified: {commit_sha[:7]}")
                
                # Additional verification: check the file content matches what we wrote
                updated_contents = self.repo.get_contents(self.build_info, ref=branch_name)
                if updated_contents.decoded_content.decode('utf-8').strip() == build_info_txt:
                    print(f"✓ Build info content verified on branch {branch_name}")
                    return result
                else:
                    print(f"⚠ Build info content mismatch on branch {branch_name}")
                    
            except Exception as e:
                print(f"⚠ Could not verify build info commit: {e}")
        raise Exception(f"✗ Error updating/validating {self.build_info} on branch {branch_name}: {result}")
        

    def check_and_update_gh_secrets(self, secrets: Dict[str, str]) -> None:
        """
        Checks if the GitHub secrets exists, deletes them if they do, and creates new ones with the given
            key value pairs.

        :param Dict[str, str] secrets: Dictionary of secrets to update
        :rtype: None
        """
        try:
            for secret, value in secrets.items():
                try:
                    if self.repo.get_secret(secret):
                        print(f"Worker {os.getpid()}: {secret} already exists, deleting...")
                        self.repo.delete_secret(secret)
                    print(f"Worker {os.getpid()}: Creating {secret}...")
                except github.GithubException as e:
                    if e.status == 404:
                        print(f"Worker {os.getpid()}: {secret} does not exist, creating...")
                    else:
                        print(f"Worker {os.getpid()}: Error checking secret {secret}: {e}")
                        raise e
                self.repo.create_secret(secret, value)
                print(f"Worker {os.getpid()}: {secret} created successfully.")
        except Exception as e:
            print(f"Worker {os.getpid()}: Error checking and updating GitHub secrets: {e}")
            raise e from e
    
    def delete_branch(self, branch_name: str) -> None:
        """
        Delete a branch if it exists.
        
        :param branch_name: Name of the branch to delete
        """
        try:
            ref = self.repo.get_git_ref(f"heads/{branch_name}")
            ref.delete()
            print(f"✓ Deleted branch: {branch_name}")
        except Exception as e:
            print(f"Branch '{branch_name}' might not exist or other error: {e}")
    
    def reset_repo_state(self, folder_name: str, keep_file_names: Optional[List[str]] = None) -> None:
        """
        Reset the repository to a clean state by clearing a folder.
        This is typically called during cleanup.

        :param str folder_name: Name of the folder to clear
        :param List[str] keep_file_names: List of file names to keep in the folder, defaults to [".gitkeep"]
        :rtype: None
        """
        # TODO: may need to use a commit to reset the repo state rather than just deleting/recreating files
        if keep_file_names is None:
            keep_file_names = [".gitkeep"]

        try:
            folder_contents = self.repo.get_contents(folder_name)
            for content in folder_contents:
                if content.name not in keep_file_names:
                    self.repo.delete_file(
                        path=content.path,
                        message=f"Clear {folder_name} folder",
                        sha=content.sha,
                        branch="main",
                    )
            
            self._iterate_directory_and_files(folder_name, keep_file_names)
            print(f"✓ {folder_name} folder reset successfully")
            
        except Exception as e:
            print(f"Error resetting repository state: {e}")
    
    def check_if_action_is_complete(self, pr_title: str, wait_before_checking: Optional[int] = 60, max_retries: Optional[int] = 10, branch_name: Optional[str] = None) -> bool:
        """
        Check if an action is complete.
        
        :param str pr_title: Title of the PR to check the action for
        :param int wait_before_checking: Time to wait before checking if the action is complete, defaults to 60 seconds
        :param int max_retries: Maximum number of retries, defaults to 10
        :param str branch_name: Name of the branch to check
        :return: True if action is complete, False otherwise
        """
        import time
        print(f"Waiting {wait_before_checking} seconds before checking if action is complete...")
        time.sleep(wait_before_checking)
        print(f"Checking if action is complete...")
        if not branch_name:
            branch_name = self.branch_name
        for retry in range(max_retries):
            workflow_runs = self.repo.get_workflow_runs(branch=branch_name)  # type: ignore
            if workflow_runs.totalCount > 0:
                if filtered_runs := [run for run in workflow_runs if run.display_title.lower() == pr_title.lower()]:
                    # check the first run in the list
                    if filtered_runs[0].status == "completed":
                        print(f"✓ Action is complete")
                        return True
                print(f"✗ Action is not complete")
            print(f"Waiting 60 seconds before checking again...{retry + 1} of {max_retries}")
            time.sleep(60)
        print(f"✗ Action is not complete after {max_retries} retries")
        return False
    
    def cleanup_requirements(self, requirements_path: str = "Requirements/") -> None:
        """
        Reset requirements.txt to blank.
        
        :param str requirements_path: Path to requirements folder
        :rtype: None
        """
        try:
            requirements_file = self.repo.get_contents(os.path.join(requirements_path, "requirements.txt"))
            # check if the file has no content before resetting it
            if requirements_file.decoded_content == b"":
                print("✓ Requirements.txt is already blank")
                return
            self.repo.update_file(
                path=requirements_file.path,
                message="Reset requirements.txt to blank",
                content="",
                sha=requirements_file.sha,
                branch="main",
            )
            print("✓ Requirements.txt reset successfully")
        except Exception as e:
            print(f"Error cleaning up requirements: {e}")
    
    def get_repo_info(self) -> Dict[str, str]:
        """
        Get repository information.
        
        :return: Dictionary with repository information
        """
        return {
            "repo_name": self.repo_name,
            "repo_url": self.repo_url,
            "default_branch": self.repo.default_branch
        }