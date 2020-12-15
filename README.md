# Template Service

This template sets up a service which has the following features:
- An API Jar, including proto files and java generated from them
- A docker image for running the service
- A helm chart for deploying the service
- A CircleCI pipeline that builds, tests and publishes each of the above to their respective repository

### Getting Started
1. Create a new repository from this one using the "Create from template" feature in github.
1. In the new repository rename each project by updating its directory names.
1. In `settings.gradle.kts`, rename the root project and the included project references to match the new project names from the previous step.
1. Rename any project dependency references in `build.gradle.kts` files to match the new project names (i.e. `api(project(":service-api"))`)
1. Update the group name across all projects by update the `subprojects` block in the root `build.gradle.kts`
1. Delete any example source code, unused dependencies in `build.gradle.kts` files, etc. Replace with own.
1. Update the application project's (`:service` in this example) `build.gradle.kts` to point to the new main class name.
1. Update helm deployment. You can put the helm folder anywhere but it is recommended you put it in the repo root for now
   so you don't have to change the CircleCI commands to validate and package the charts.
1. Replace this file with a more useful README!

### Future work
Turn the helm commands into a gradle plugin so we all use gradle for everything! Tim will look at the gradle plugin "unbroken-dome.helm" https://plugins.gradle.org/plugin/org.unbroken-dome.helm
