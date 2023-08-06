import pkg_resources

# List all installed packages and their versions
installed_packages = [f"{dist.project_name}=={dist.version}" for dist in pkg_resources.working_set]

# Write the package list to a requirements.txt file
with open("requirements.txt", "w") as requirements_file:
    requirements_file.write("\n".join(installed_packages))

print("requirements.txt file generated.")
