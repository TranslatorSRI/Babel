# Dockerfile for building a Babel image.

# Let's pick up the latest Debian-based Python image.
FROM python:latest

# Configuration options:
# - ${ROOT} is where Babel source code will be copied.
ARG ROOT=/code/babel
# - ${CORES} is the default number of cores to use.
ARG CORES=5

# Upgrade system files.
RUN apt update
RUN apt -y upgrade

# Install or upgrade some prerequisite packages.
RUN apt install -y python3-venv
RUN apt install -y gcc
RUN apt install -y git
RUN pip3 install --upgrade pip

# The following packages are useful in debugging runs
# of this software on a Kubernetes cluster, but can
# be removed if not needed.
RUN apt-get install -y htop
RUN apt-get install -y screen
RUN apt-get install -y vim
RUN apt-get install -y rsync
RUN apt-get install -y jq

# Create a non-root-user.
RUN adduser --home ${ROOT} --uid 1000 nru

# Copy directory into Docker.
COPY --chown=nru . ${ROOT}

# We can download some source files that Babel would otherwise need to download
# later. This means that this Docker will need to be rebuilt whenever these files
# change. They can be commented out if you would like the Docker to download these
# files when it is started.
#ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene2ensembl.gz babel_downloads/NCBIGene/gene2ensembl.gz
#ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz babel_downloads/NCBIGene/gene_info.gz
#ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene_orthologs.gz babel_downloads/NCBIGene/gene_orthologs.gz
#ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene_refseq_uniprotkb_collab.gz babel_downloads/NCBIGene/gene_refseq_uniprotkb_collab.gz
#ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/mim2gene_medgen babel_downloads/NCBIGene/mim2gene_medgen
#ADD --chown=runner https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz babel_downloads/PUBCHEM.COMPOUND/CID-SMILES.gz

# RENCI Python Image creates a `nru` (non-root user)
# for running code.
RUN mkdir -p ${ROOT}
WORKDIR ${ROOT}
USER nru

# The RENCI Python Image doesn't create a home directory,
# which is needed by Snakemake. So we make one ourselves.
RUN mkdir ${ROOT}/home
ENV HOME=${ROOT}/home

# Create and activate a local Python venv
ENV VIRTUAL_ENV=${ROOT}/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Install requirements from the lockfile.
# RUN pip3 install -r requirements.lock
RUN pip3 install -r requirements.txt

# Our default entrypoint is to start the Babel run.
ENTRYPOINT ["bash", "-c", "snakemake --cores ${CORES}"]
