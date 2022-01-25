# Dockerfile for running the Babel build.

# We use Debian as our basic system (since that's what I'm most familiar with).
FROM debian:11

# Install software we need to run the remaining code.
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN apt-get install -y gcc
RUN apt-get install -y git
RUN pip3 install --upgrade pip

# We install some additional software while building this Docker.
# Once built, we no longer need them.
RUN apt-get install -y htop

# Set up a local user account for running Babel.
RUN useradd -U -m runner
USER runner
WORKDIR /home/runner/babel

# Copy directory into Docker.
COPY --chown=runner . /home/runner/babel

# We can download some source files that Babel would otherwise need to download
# later. This means that this Docker will need to be rebuilt whenever these files
# change. They can be commented out if you would like the Docker to download these
# files when it is started.
ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene2ensembl.gz babel_downloads/NCBIGene/gene2ensembl.gz
ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz babel_downloads/NCBIGene/gene_info.gz
ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene_orthologs.gz babel_downloads/NCBIGene/gene_orthologs.gz
ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/gene_refseq_uniprotkb_collab.gz babel_downloads/NCBIGene/gene_refseq_uniprotkb_collab.gz
ADD --chown=runner https://ftp.ncbi.nih.gov/gene/DATA/mim2gene_medgen babel_downloads/NCBIGene/mim2gene_medgen

# Make sure installed Python packages are on the PATH
ENV PATH="/home/runner/.local/bin:${PATH}"

# Install requirements
RUN pip3 install -r requirements.txt

# Our default entrypoint is to start the Babel run.
# nproc returns the number of available cores; we use one less than that.
ENTRYPOINT bash -c 'snakemake -c $(($(nproc)-1))'
