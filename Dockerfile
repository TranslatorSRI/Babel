# Dockerfile for running the Babel build.

# Set up and update Anaconda.
FROM conda/miniconda3
RUN conda update conda python

# Install software we need to run the remaining code.
RUN conda install git
RUN conda install pip

# Set up a local user account for running Babel.
RUN useradd -m runner
USER runner
WORKDIR /home/runner

# Copy directory into Docker.
COPY . /home/runner

# Install requirements
RUN pip3 install -r requirements.txt

# Our default entrypoint is to start the Babel run.
# nproc returns the number of available cores; we use one less than that.
ENTRYPOINT bash -c 'snakemake -c $(($(nproc)-1))}'
