# Dockerfile for running the Babel build.

# We use Debian as our basic system (since that's what I'm most familiar with).
FROM debian:11

# Install software we need to run the remaining code.
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN apt-get install -y gcc
RUN apt-get install -y git
RUN pip3 install --upgrade pip

# Set up a local user account for running Babel.
RUN useradd -U -m runner
USER runner
WORKDIR /home/runner/babel

# Copy directory into Docker.
COPY --chown=runner . /home/runner/babel

# Make sure installed Python packages are on the PATH
ENV PATH="/home/runner/.local/bin:${PATH}"

# Install requirements
RUN pip3 install -r requirements.txt

# Our default entrypoint is to start the Babel run.
# nproc returns the number of available cores; we use one less than that.
ENTRYPOINT bash -c 'snakemake -c $(($(nproc)-1))'
