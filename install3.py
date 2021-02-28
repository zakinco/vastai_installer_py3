#!python3
import inspect
import errno
import glob
import random
import datetime
import hashlib
import shutil
import string
import re
import pipes
import subprocess
import os
import argparse
import time
from contextlib import contextmanager
import sys
import math
import traceback

sys.argv[0] = os.path.abspath(sys.argv[0])


class InstallFailed(Exception):
    pass


DAEMON_USER = "vastai_kaalia"
NVIDIA_CHECK_FILE = "/proc/driver/nvidia/version"
NVIDIA_DIR = "NVIDIA-Linux-x86_64-{version}"
NVIDIA_FILE = "NVIDIA-Linux-x86_64-{version}.run"
DATA_DIR = "/var/lib/vastai_kaalia"
UPDATE_SERVER = "https://s3.amazonaws.com/vast.ai/static"
MEGABYTE = 1024*1024

DOCKER_TMPDIR = "/var/lib/docker-temporarily-renamed/"
DOCKER_DIR_NOS = "/var/lib/docker"
DOCKER_DIR = DOCKER_DIR_NOS + "/"
DOCKER_LOOP = "/var/lib/docker-loop.xfs"
restore_lightdm = False
logfile = None
logfile_name = None

maybe_sudo = ["sudo"] if os.geteuid() != 0 else []
for x in ["LANG", "LC_ADDRESS", "LC_COLLATE", "LC_CTYPE", "LC_IDENTIFICATION", "LC_MONETARY", "LC_MEASUREMENT", "LC_NAME", "LC_NUMERIC", "LC_PAPER", "LC_TELEPHONE", "LC_TIME", "LC_ALL", "LANGUAGE"]:
    os.environ[x] = "C.UTF-8"
os.environ["LC_MESSAGES"] = "C"


def upload_logs(success, upload_filename=None, out_dir=None, extra_files=(), silence_apt=True):
    print()
    try:
        if logfile is not None:
            logfile.flush()
            os.fsync(logfile.fileno())
    except:
        traceback.print_exc()
    try:
        if out_dir is None:
            out_dir = orig_dir
        if upload_filename is None:
            upload_filename = out_dir + "/vastai_install_logs.tar.gz"
    except:
        upload_filename = "/tmp/vastai_install_logs_" + str(int(time.time())) + ".tar.gz"
        traceback.print_exc()
    try:
        fallback_filename = args.logfile
    except:
        fallback_filename = None
        traceback.print_exc()
    try:
        subprocess.call(["tar", "--ignore-failed-read", "-czf", upload_filename]
                        + glob.glob("/var/lib/vastai_kaalia/*.log")
                        + glob.glob("/var/lib/vastai_kaalia/*.sh")
                        + glob.glob("/var/lib/vastai_kaalia/*/*.log")
                        + glob.glob("/var/lib/vastai_kaalia/*update*")
                        + glob.glob("/var/lib/vastai_kaalia/launch*")
                        + glob.glob("/var/log/syslog")
                        + glob.glob("/var/log/apt")
                        + glob.glob("/etc/apt/sources.list*")
                        + glob.glob(args.logfile), stdout=open("/dev/null", "w"), stderr=open("/dev/null", "w"), env=os.environ)
        if success is None:
            print("\033[33;1m Sending logs...")
        elif not success:
            print("\033[33;1m Install failed. Relevant logs have been put into")
            print("\033[33;1m the file vastai_install_logs.tar.gz.\033[m")
    except:
        upload_filename = fallback_filename
        traceback.print_exc()
    server = None
    try:
        server = args.vast_server
    except:
        traceback.print_exc()
    if not server:
        server = "https://vast.ai"
    upload_url = server + "/upload_logs/?"
    try:
        upload_url += "api_key=" + args.api_key + "&"
    except:
        traceback.print_exc()
    now = int(time.time())
    if success:
        upload_url += "fn=install_success_{}.tar.gz".format(now)
    elif success is None:
        upload_url += "fn=log_dump_{}.tar.gz".format(now)
    else:
        upload_url += "fn=install_failure_{}.tar.gz".format(now)

    def do():
        # TODO: don't depend on curl to do this (maybe?)
        subprocess.call(["curl", '--header', 'Content-Type: application/octet-stream', '--data-binary', "@" + upload_filename, "-X", "POST", upload_url], stdout=open("/dev/null", "w"), stderr=open("/dev/null", "w"), env=os.environ)

    if upload_filename is not None:
        try:
            do()
        except OSError as e:
            if e.errno != errno.ENOENT:
                traceback.print_exc()
                return
            apt_stdout = None
            if silence_apt:
                apt_stdout = open("/dev/null", "w")
            subprocess.call(maybe_sudo + ["apt-get", "-qq", "install", "curl"], stdout=apt_stdout, stderr=apt_stdout, env=os.environ)
            do()
        except:
            traceback.print_exc()


orig_dir = os.path.abspath(".")


def format_process_call(*a, **kw):
    res = []
    for x in a:
        res.append(repr(x))
    for k, v in list(kw.items()):
        if k in ["stdout", "stderr"] and v is logfile:
            continue
        res.append("{}={}".format(k, repr(v)))
    return ", ".join(res)


def process_call(*a, **kw):
    kw.setdefault("stdout", logfile)
    kw.setdefault("stderr", logfile)
    optional = kw.pop("optional", False)
    kw["env"] = os.environ
    try:
        log("subprocess.call({})".format(format_process_call(*a, **kw)), level=4)
        return subprocess.call(*a, **kw)
    except OSError as e:
        if e.errno == errno.ENOENT and optional:
            return 1
        raise


def process_check_call(*a, **kw):
    kw.setdefault("stdout", logfile)
    kw.setdefault("stderr", logfile)
    kw["env"] = os.environ
    log("subprocess.check_call({})".format(format_process_call(*a, **kw)), level=4)
    return subprocess.check_call(*a, **kw)


def process_check_output(*a, **kw):
    # kw.setdefault("stdout", logfile)
    kw.setdefault("stderr", logfile)
    log("subprocess.check_output({})".format(format_process_call(*a, **kw)), level=4)
    kw["env"] = os.environ
    result = subprocess.check_output(*a, **kw)
    log("output:", result, level=5)
    return result.decode()


def process_open(*a, **kw):
    log("subprocess.Popen({})".format(format_process_call(*a, **kw)), level=4)
    kw["env"] = os.environ
    return subprocess.Popen(*a, **kw)


def as_gib(bytecount):
    return "{0:.2f} GiB".format(bytecount/float(1024*1024*1024))


def quantize(x, y):
    return int(x/float(y))*y


@contextmanager
def tempdir():
    import tempfile
    d = tempfile.mkdtemp()
    log("tempdir: yielding", d, level=5)
    yield d
    log("tempdir: deleting", d, level=5)
    os.rmdir(d)
    assert not os.path.isdir(d)
    log("tempdir: done deleting", d, level=5)


@contextmanager
def color(c):
    sys.stdout.write('\033[' + str(c) + 'm')
    sys.stdout.flush()
    yield
    sys.stdout.write('\033[m')
    sys.stdout.flush()


def ceil(x, size):
    return x + (size - x) % size


def floor(x, size):
    x = x - 1
    return x - x % size


def get_partitions(devices):
    free_zones = []
    partitions = {}
    for dev in devices:
        # print(process_check_output(["parted", "-s", dev, "unit", "B", "print", "free"]))
        out = process_check_output(["parted", "-s", "-m", dev, "unit", "B", "print", "free"], stderr=logfile)
        rows = []
        dtype = None
        for line in out.split("\n"):
            if not line:
                continue
            if line.startswith("BYT"):
                continue
            if not line.endswith(";"):
                continue
            row = line[:-1].split(":")
            if not rows:
                dtype = row[5]
            rows.append(row)

            if row[4] == "free":
                start = ceil(int(row[1].strip("B")), MEGABYTE)
                end = floor(int(row[2].strip("B")), MEGABYTE)
                if start >= end:
                    continue
                free_zones.append((end-start, dev, dtype, start, end))
            elif row[0] == dev:
                continue
            else:
                index = int(row[0])
                partitions[dev, index] = (dtype,) + tuple(row)
    print(dtype)
    # sys.exit()
    return free_zones, partitions


def diff_partitions(partitions, partitions_2):
    new_partitions = set()
    # error = False
    try:
        d = "/dev/disk/by-partlabel/vast_client_data"
        assert os.readlink(d)
        return os.path.realpath(d)
    except OSError as e:
        log("OSError reading partition link:", e, level=1)

    for key, value in list(partitions_2.items()):
        if key not in partitions:
            if len(new_partitions):
                with red():
                    log("got multiple new partitions during partition create, don't know which one is the real new one! abandoning attempt to create a partition, using loopback instead.", level=-1)
                return None
            new_partitions.add(key)
            continue
        if partitions[key] != value:
            with red():
                log("warning: partition {} changed during creation of new partition! don't know which one is the real new one! abandoning attempt to create a partition, using loopback instead.".format(key), level=-1)
            return None
    if not len(new_partitions):
        with red():
            log("warning: disk {} appeared to be ampty empty after creation of new partition! abandoning attempt to create a partition, using loopback instead.".format(key), level=-1)
        return None

    dev, idx = list(new_partitions)[0]
    # TODO IMPORTANT should check if the partition id and /dev id really match
    if "nvme" in dev:
        new_partition = str(dev) + "p" + str(idx)
    else:
        new_partition = str(dev) + str(idx)
    return new_partition


def allocate_partition(minsize):
    out = process_check_output(["parted", "-sl"], stderr=logfile)
    devices = re.findall("^Disk (/dev[^:]+)", out, re.MULTILINE)

    free_zones, partitions = get_partitions(devices)

    if not free_zones:
        with red():
            print("No unpartitioned zones to create a partition. Will attempt a loopback partition; this will have significantly worse performance.")
        return None

    size, dev, dtype, start, end = sorted(free_zones)[-1]

    process_check_call(["apt-get", apt_q, "install", "-y", "xfsprogs"], stdout=apt_stdout, stderr=apt_stdout)
    if size < minsize:
        with red():
            print("No large enough unpartitioned zones to create a partition. Will attempt a loopback partition; this will have significantly worse performance.")
        return None

    if dtype == "gpt":
        process_call(["parted", "-sm", dev, "mkpart", "vast_client_data", "xfs", str(start)+"B", str(end)+"B"], stdout=logfile, stderr=logfile)
    else:
        process_call(["parted", "-sm", dev, "mkpart", "primary", "xfs", str(start)+"B", str(end)+"B"], stdout=logfile, stderr=logfile)

    free_zones_2, partitions_2 = get_partitions(devices)
    new_partition = diff_partitions(partitions, partitions_2)
    return new_partition


def get_mounts(dict_field=0):
    try:
        with open("/proc/mounts", "r") as reader:
            lines = [x for x in reader.read().split("\n") if x.strip()]
        res = {}
        for line in lines:
            log("/proc/mounts row:", line, level=7)
            row = line.split(" ", 4)
            res[row[dict_field]] = row
        return res
    except (OSError, IOError):
        return {}


def ensure_mount(directory):
    directory = os.path.abspath(directory)
    log("Attempting mount of", directory, level=2)
    mounts = {os.path.abspath(x): y for x, y in list(get_mounts(dict_field=1).items())}
    row = None
    if directory in mounts:
        log("partition already mounted:", mounts[directory], level=5)
        row = mounts[directory]
    else:
        log("mounting", directory, level=4)
        process_call(["mount", directory], stdout=logfile, stderr=logfile)
        log("mounting done, checking type", level=4)
        mounts = {os.path.abspath(x): y for x, y in list(get_mounts(dict_field=1).items())}
        if directory in mounts:
            row = mounts[directory]
        else:
            log("partition doesn't appear mounted", level=4)
    log("mount row:", row, level=5)
    return row


def check_mountability(new_partition):
    if new_partition is None:
        return None
    log("Checking mountability of", new_partition, level=2)
    mounts = get_mounts()
    mounted_type = None
    if new_partition in mounts:
        log("new partition mounted:", mounts[new_partition], level=5)
        process_check_call(["umount", new_partition], stdout=logfile, stderr=logfile)
        mounted_type = mounts[new_partition][2]
    else:
        with tempdir() as d:
            log("mounting", new_partition, "on", d, level=4)
            err = process_call(["mount", new_partition, d], stdout=logfile, stderr=logfile)
            log("mounting done, checking type", level=4)
            mounts = get_mounts()
            if new_partition in mounts:
                mounted_type = mounts[new_partition][2]
                log("found type", mounted_type, level=4)
            else:
                log("partition doesn't appear mounted", level=4)
            if not err:
                process_check_call(["umount", d], stdout=logfile, stderr=logfile)
    log("found type", mounted_type, "for", new_partition, level=3)
    return mounted_type


def get_file_size(filename):
    "Get the file size by seeking at end"
    fd = os.open(filename, os.O_RDONLY)
    try:
        return os.lseek(fd, 0, os.SEEK_END)
    finally:
        os.close(fd)


def makepartition(minsize=1024*1024*1024*20):
    try:
        d = "/dev/disk/by-partlabel/vast_client_data"
        assert os.readlink(d)
        new_partition = os.path.realpath(d)
        if get_file_size(d) < minsize:
            return None
    except OSError as e:
        log("OSError reading partition link, will assume it doesn't exist. error:", e, level=1)
        new_partition = allocate_partition(minsize)

    if new_partition is None:
        return None
    t = check_mountability(new_partition)
    log("known type:", t)
    if t is None:
        process_call(["mkfs.xfs", new_partition], stdout=logfile, stderr=logfile)
    elif t != "xfs":
        log("WARNING: EXISTING FILESYSTEM FOUND ON", new_partition, "- not attempting formatting. Contact vast.ai support.", level=-1)
        raise Exception
    uuid = process_check_output(["blkid", "-s", "UUID", "-o", "value", new_partition], stderr=logfile)
    filename = 'UUID="{}"'.format(uuid.strip())
    return filename


def create_loop(DOCKER_LOOP):
    if not os.path.exists(DOCKER_LOOP):
        with green():
            log("=> Create loop file", level=2)  # green
        with open(DOCKER_LOOP, "w") as writer:
            writer.seek(storage_size-1)
            writer.write('\0')
        with green():
            log("=> Put xfs in it", level=2)  # green
        process_check_call(["mkfs.xfs", DOCKER_LOOP], stdout=logfile, stderr=logfile)
    return DOCKER_LOOP


def get_nvidia_version():
    if not os.path.exists(NVIDIA_CHECK_FILE):
        with green():
            log("=> ** No nvidia driver installed, installing", level=1)
        return None
    with open(NVIDIA_CHECK_FILE, "r") as reader:
        contents = reader.read()

    substr = re.search(r"[mM]odule *([0-9]+\.[0-9]+)", contents)
    if not substr:
        return None
    return substr.group(1)


def ask_user_nvidia(args, installed_nvidia):

    nvidia_versions = [
        ("450.57", "d50c77fc4fda2a5c5ab2af64524da8a3214077bd7daf0dbf7c1986e0ca05d711"),
        ("440.82", "edd415acf2f75a659e0f3b4f27c1fab770cf21614e84a18152d94f0d004a758e"),
        ("410.78", "5db64b57cce95331eed0bcdbdd7faa43732f3a2a014fa5a2d3af24b8ab5d2ab2"),
        ("410.73", "bebc9cf781201beb5ec1a1dde7672db68609b8af0aa5ff32daa3ebb533c2ff1e"),
        ("410.66", "8fb6ad857fa9a93307adf3f44f5decddd0bf8587a7ad66c6bfb33e07e4feb217"),
        ("390.48", "2c01f57abd78e9c52d3e76e3cdf688835be54f419a1093bbaa7ab21f375d4399"),
        ("384.130", "e115c70683a7d0e36c75b4210b07019605e2c31615dae599766797fe132d1973"),
        ("390.42", "4aad7a9ade4f6e2656a3159c466f922b69daa7b68278d132676f83417bac010e"),
        ("390.25", "19014642b1c2cc81e2bb55e98b6b0ef7ea2737fadef68a0ba5739b5a83a61a72"),
        ("340.107", "e3e359964ed568008584c4fb2dbcb76b76a848591d1456a783e94dd0c7f6695f"),
        ("340.106", "6cf90d28b1505348da2d75d12f4f878bc91b474bf71254d7b6acee10c3bbcb5b"),
        ("384.111", "85886d52d37ff03ba84946a407638f43171e409a50aed52149e1b67b92e017b1"),
        ("387.34", "e48f9368a31204c8125428eaee4f1bb7c2988414689faeccaf1e90050d6f881b"),
        ("384.98", "fb10b66d9a835c8a5fca9bf2aeb2a240732108b1d6fd11dea19c326463c6b6bb"),
        ("387.12", "0c167561403278a14d5b4ef0a2b1ddaa6b561ecccd020e7359a41cf8702b3630"),
        ("384.90", "0ddf6820f2fcca3ec3021e42a028f8bc08bca123fcea4c0c3f41c8c0ffa5febd"),
    ]
    while args.driver_version is None:
        print("What version of the nvidia driver would you like? Known versions:")
        versions = {}
        has_installed_vers = installed_nvidia in [version for version, hash in nvidia_versions]
        if not has_installed_vers and installed_nvidia is not None:
            nvidia_versions.append([installed_nvidia, None])

        for idx, x in enumerate(nvidia_versions):
            version, hash = x
            character = string.ascii_lowercase[idx]
            versions[character] = x
            installed = " (installed)" if version == installed_nvidia else ""
            print("    {option}:    {version}{installed}".format(option=character, version=version, installed=installed))
        print()
        choice = input("Your choice (letter or version number): ").strip()
        if "." in choice:
            args.driver_version = choice
        else:
            x = versions.get(choice)
            if x is not None:
                args.driver_version = x[0]
    if "--driver-version" not in sys.argv:
        sys.argv.append("--driver-version")
        sys.argv.append(args.driver_version)
    driver_hash = dict(nvidia_versions).get(args.driver_version, None)
    return driver_hash


def need_nvidia_install(args, installed_nvidia):
    if installed_nvidia != args.driver_version:
        with green():
            log("=> ** Installed nvidia version {x} does not match {exp}, reinstalling".format(x=installed_nvidia, exp=args.driver_version), level=1)
        return True
    return False


def get_file_hash(filename):
    h = hashlib.sha256()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(1024)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def get_last_apt_get_update():
    st = os.stat('/var/cache/apt')
    return time.time() - st.st_mtime


def run_apt_get_update():
    last_apt_get_update = get_last_apt_get_update()

    # Default To 24 hours
    update_interval = 24 * 60 * 60

    if last_apt_get_update > update_interval:
        with green():
            log("=> Do apt update", level=1)
        process_call(["apt-get", apt_q, "update"], stdout=apt_stdout, stderr=apt_stdout)
        with green():
            log("=> Done with apt update", level=2)
    else:
        with green():
            log("=> Skip apt-get update because its last run was {} ago".format(datetime.timedelta(seconds=last_apt_get_update)), level=2)


def docker_install(allow_partitioning):
    with green():
        log("=> Remove any non-docker-ce installation", level=2)  # green
    process_call(["apt-get", apt_q, "remove", "docker", "docker-engine", "docker.io"], stdout=apt_stdout, stderr=apt_stdout)
    mount_success = False
    in_fstab = (DOCKER_DIR_NOS in open("/etc/fstab", "r").read())
    if in_fstab:
        # process_call(["mount", DOCKER_DIR], stdout=logfile, stderr=logfile)
        row = ensure_mount(DOCKER_DIR)
        if row:
            assert row[2] == "xfs"
            assert "prjquota" in row[3]
            mount_success = True
        else:
            mount_success = False

    if not mount_success:
        do_rename = os.path.exists(DOCKER_DIR)
        if do_rename:
            with green():
                log("=> Shut down existing docker-ce", level=2)  # green
            process_call(["systemctl", "stop", "docker"], stdout=apt_stdout, stderr=apt_stdout)
            with green():
                log("=> Temporarily move existing docker subtree", level=2)  # green
            os.rename(DOCKER_DIR, DOCKER_TMPDIR)

        with green():
            log("=> Install xfs", level=1)  # green
        process_check_call(["apt-get", apt_q, "install", "-y", "xfsprogs"], stdout=apt_stdout, stderr=apt_stdout)

        filename = None
        if allow_partitioning:
            filename = makepartition()
        use_loop = False
        if filename is None:
            use_loop = True
            filename = create_loop(DOCKER_LOOP)

        with open("/etc/fstab", "r") as reader:
            existing_fstab = reader.readlines()
        existing_fstab = [x for x in existing_fstab if DOCKER_DIR not in x.partition("#")[0]]
        existing_fstab.append("\n{file} {dir} xfs {loop}rw,auto,pquota 0 0\n"
                              .format(file=filename, dir=DOCKER_DIR, loop="loop," if use_loop else ""))
        with open("/etc/fstab", "w") as writer:
            writer.write("".join(existing_fstab))
        try:
            os.makedirs(DOCKER_DIR)
        except OSError:
            pass
        assert os.listdir(DOCKER_DIR) == []

        row = ensure_mount(DOCKER_DIR)
        assert row[2] == "xfs"
        assert "prjquota" in row[3]

        if do_rename:
            with green():
                log("=> Copy old docker data into new storage", level=2)  # green
            process_check_call(["rsync", "-XHAa", DOCKER_TMPDIR, DOCKER_DIR], stdout=logfile, stderr=logfile)
            with green():
                log("=> Clean up now-duplicate old docker data", level=2)  # green
            process_check_call(["rm", "-rf", DOCKER_TMPDIR], stdout=logfile, stderr=logfile)
            process_call(["systemctl", "start", "docker"], stdout=logfile, stderr=logfile)
    # process_check_call(["mountpoint", DOCKER_DIR, "-q"], stdout=logfile, stderr=logfile)
    # raise SystemExit(0)

    with green():
        log("=> install apt stuff needed to do custom repos", level=2)  # green
    process_call(["apt-get", apt_q, "update"], stdout=apt_stdout, stderr=apt_stdout)
    process_check_call(["apt-get", apt_q, "install", "-y", "apt-transport-https", "ca-certificates", "curl", "software-properties-common"], stdout=apt_stdout, stderr=apt_stdout)

    # set up docker and nvidia-docker repos
    with green():
        log("=> Install repositories for docker and nvidia-docker", level=1)  # green
    process_check_call("curl -qfsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -", shell=True, stdout=apt_stdout, stderr=apt_stdout)
    process_check_call(["apt-key", "fingerprint", "0EBFCD88"], stdout=apt_stdout, stderr=apt_stdout)

    # info: lsb_release -cs
    process_check_call(["add-apt-repository",
                        "deb [arch=amd64] https://download.docker.com/linux/ubuntu "
                        + os_release
                        + " stable"], stdout=apt_stdout, stderr=apt_stdout)
    process_call("curl -qs -L https://nvidia.github.io/nvidia-docker/gpgkey | apt-key add -", shell=True, stdout=apt_stdout, stderr=apt_stdout)
    distribution = process_check_output(["bash", "-c", '. /etc/os-release;echo $ID$VERSION_ID'], stderr=logfile).strip()
    docker_list = process_check_output(["curl", "-qs", "-L", "https://nvidia.github.io/nvidia-docker/"+distribution+"/nvidia-docker.list"], stderr=logfile)
    with open("/etc/apt/sources.list.d/nvidia-docker.list", "w") as writer:
        writer.write(docker_list)
    process_call(["apt-get", apt_q, "update"], stdout=apt_stdout, stderr=apt_stdout)

    with green():
        log("=> Install docker and nvidia-docker", level=1)  # green
    process_check_call(["apt-get", apt_q, "install", "-y", "nvidia-docker2"], stdout=apt_stdout, stderr=apt_stdout)
    process_check_call(["systemctl", "enable", "docker"], stdout=apt_stdout, stderr=apt_stdout)
    process_check_call(["service", "docker", "start"], stdout=apt_stdout, stderr=apt_stdout)
    process_check_call(["pkill", "-SIGHUP", "dockerd"], stdout=apt_stdout, stderr=apt_stdout)

    with green():
        log("=> Create docker group", level=1)  # green

    err = process_call(["groupadd", "docker"], stdout=logfile, stderr=logfile)
    if err != 0:
        with green():
            log("docker group exists, not creating - this is ok", level=2)


def log(*a, **kw):
    level = kw.pop("level", 3)
    time = "[{} UTC]".format(datetime.datetime.utcnow().isoformat(" "))
    try:
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        debuginfo = ("%s():%d:" % (caller.function, caller.lineno,))
    except Exception:
        import traceback
        traceback.print_exc()
        debuginfo = ""
    if args.verbose >= level:
        if args.verbose > 4:
            print(time, debuginfo, *a, **kw)
        else:
            print(*a, **kw)
        if args.verbose:
            sys.stdout.flush()
    if logfile is not None:
        kw["file"] = logfile
        logfile.flush()
        os.fsync(logfile.fileno())
        print(time, debuginfo, *a, **kw)
        logfile.flush()
        os.fsync(logfile.fileno())


def red():
    return color(31)


def green():
    return color(32)


def yellow():
    return color(33)


def blue():
    return color(34)


try:
    if __name__ == "__main__":
        statvfs = os.statvfs('/var/lib/')
        freespace = statvfs.f_frsize * statvfs.f_bavail
        default_storage = quantize(freespace * 0.95 - 1024*1024*1024, 1024*1024)

        # red    = lambda: color(31)
        # green  = lambda: color(32)
        # yellow = lambda: color(33)
        # blue   = lambda: color(34)

        print("DEBUG")

        parser = argparse.ArgumentParser()
        parser.add_argument("api_key")
        parser.add_argument("--logs", action="store_true")
        parser.add_argument("--launched-in-vt", action="store_true")
        parser.add_argument("--agree-to-nvidia-license", action="store_true")
        parser.add_argument("--driver-version", default=None)
        parser.add_argument("--no-driver", action="store_true", help="assume nvidia driver is installed")
        parser.add_argument("--no-partitioning", action="store_false", dest="allow_partitioning", help="do not attempt to create xfs disk partitions in unpartitioned space")
        parser.add_argument("--reset-machine", action="store_true", help="Reset machine id")
        parser.add_argument("--no-daemon", action="store_true", help="don't run vast.ai daemon installer, just set up necessary environment")
        parser.add_argument("--no-docker", action="store_true", help="assume docker is configured in exactly the way needed by vast.ai already")
        parser.add_argument("--storage-size", default=None, type=float, help="set size of loopback file in GiB for loopback case (not preferred)")
        parser.add_argument("--update-server", default=None)
        parser.add_argument("--vast-server", default=None)
        parser.add_argument("-l", "--logfile", default="vast_host_install.log")
        parser.add_argument("-d", "--dev", nargs="?", default=None, const=True)
        parser.add_argument("-v", "--verbose", action="count", default=0)

        # args, install_update, package, root, logfile, apt_q, apt_stdout, wget_flags, storage_size, warn, freespace_after, installed_nvidia, driver_hash, daemon_auth
        # nvidia_file, nvidia_dir, answer, license_agreement, update_vars, update_launcher, cycle, progress, now, unwait
        args = parser.parse_args()
        install_update = None
        package = None
        if args.dev is True:
            d = os.path.dirname(os.path.abspath(__file__))
            root = os.path.dirname(d)
            install_update = pipes.quote(os.path.join(d, "install_update.sh"))
            package = pipes.quote(os.path.join(root, "package.tar.gz"))
            if args.vast_server is None:
                args.vast_server = "http://localhost:6543"
        elif args.dev:
            args.vast_server = args.dev
            args.update_server = args.dev + "/public"
        args.logfile = os.path.abspath(os.path.expanduser(args.logfile))
        logfile_name = args.logfile
        logfile = open(args.logfile, "a")
        if "VAST_DEBUG" in os.environ and os.environ["VAST_DEBUG"].strip():
            try:
                args.verbose = int(os.environ["VAST_DEBUG"].strip())
            except ValueError:
                args.verbose = 10
            filtered_args = []
            for arg in sys.argv:
                if arg.startswith("-") and len(arg) > 1 and arg[1] != "-":
                    flags = arg[1:]
                    flags = flags.replace("q", "")
                    if not flags:
                        continue
                    filtered_args.append("-" + flags)
                else:
                    filtered_args.append(arg)
            filtered_args.append("-" + "v" * args.verbose)
            sys.argv = filtered_args
        if args.verbose:
            os.environ["VAST_DEBUG"] = str(args.verbose)
        if args.update_server is not None:
            UPDATE_SERVER = args.update_server
        apt_q = "-y"
        apt_stdout = None
        if args.verbose == 3:
            apt_q = "-q"
        elif args.verbose == 2:
            apt_q = "-qq"
        elif args.verbose < 2:
            apt_stdout = logfile
        if args.verbose == 0:
            wget_flags = "-qc"
        else:
            wget_flags = "-c"

        os_release = process_check_output(["lsb_release", "-cs"], stderr=logfile).strip()

        if args.api_key == "logs":
            args.logs = True
        if args.logs:
            args.no_driver = True

        warn = False
        if args.storage_size is None:
            storage_size = default_storage
            warn = True
        else:
            storage_size = quantize(args.storage_size*1024*1024*1024, 1024*1024)
        freespace_after = freespace-storage_size

        installed_nvidia = get_nvidia_version()

        if not args.no_driver:
            driver_hash = ask_user_nvidia(args, installed_nvidia)

        if os.geteuid() != 0:
            args_ = ["sudo", sys.executable] + sys.argv
            print("Elevating to root permissions: Rerunning with `{args}`...".format(args=" ".join([pipes.quote(x) for x in args_])))
            proc = process_open(args_)
            try:
                sys.exit(proc.wait())
            except KeyboardInterrupt:
                proc.wait()
            sys.exit(0)

        if args.logs:
            upload_logs(success=None)
            sys.exit(0)

        with green():
            log('=> Begin vast host software install', level=0)

        if process_call(["id", "-u", DAEMON_USER], stdout=logfile, stderr=logfile) != 0:
            with green():
                log("=> Create Vast.ai daemon user", level=0)
            try:
                os.makedirs(DATA_DIR)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            with blue():
                process_call(["groupadd", "docker"], stdout=logfile, stderr=logfile)
                process_check_call([
                    "adduser",
                    "--system",
                    "--gecos",
                    "",
                    "--home",
                    DATA_DIR,
                    "--no-create-home",
                    "--disabled-password",
                    "--ingroup",
                    "docker",
                    "--shell",
                    "/bin/bash",
                    DAEMON_USER,
                    ],
                                   stdout=logfile,
                                   stderr=logfile,
                                   )
            process_check_call(["chown", "vastai_kaalia:docker", DATA_DIR, "-R"], stdout=logfile, stderr=logfile)
            with open("/etc/sudoers", "a") as appender:
                appender.write(
                    '# To allow Vast.ai host daemon to administer configuration\n'
                    + 'vastai_kaalia ALL=(ALL) NOPASSWD:ALL\n'
                )

        d = os.getcwd()
        os.chdir(DATA_DIR)

        with green():
            log("=> Configure Vast.ai daemon", level=0)

        with open("api_key", "w") as writer:
            writer.write(args.api_key)
        if not os.path.exists("machine_id") or args.reset_machine:
            with open("machine_id", "w") as writer:
                writer.write("".join(random.SystemRandom().choice("abcdef0123456789") for x in range(64)))

        update_vars = ""
        if args.vast_server is not None:
            update_vars += "export VAST_SERVER=" + pipes.quote(args.vast_server) + "\n"
        if args.update_server is not None:
            update_vars += "export UPDATE_SERVER=" + pipes.quote(args.update_server) + "\n"

        dev_extra = ""
        if args.dev:
            dev_extra = (
                "cp -f {install_update} install_update.sh\n" +
                "export PACKAGE_PATH={package_path}"
            ).format(package_path=package, install_update=install_update)

        update_launcher = (
            "#!/bin/bash\n" +
            "cd ~vastai_kaalia\n" +
            "wget {wget_flags} {update_url}$(cat ~/.channel 2>/dev/null)/update -O install_update.sh\n" +
            "{dev_extra}\n" +
            "chmod +x install_update.sh\n"
            "{vars}\n" +
            "./install_update.sh \"$@\""
        ).format(update_url=UPDATE_SERVER, vars=update_vars, wget_flags=("-q" if not args.verbose else ""), dev_extra=dev_extra)
        with open("update_launcher.sh", "w") as writer:
            writer.write(update_launcher)

        with red():
            process_check_call(["chown", "vastai_kaalia:nogroup", DATA_DIR, "-R"], stdout=logfile, stderr=logfile)

        if not args.no_daemon:
            with green():
                log("=> Update Vast.ai daemon", level=0)
            process_check_call(["su", DAEMON_USER, "-c", 'VAST_DEBUG={} bash update_launcher.sh setup'.format(str(args.verbose))])

        daemon_auth = args.api_key
        with green():
            log('=> Checking for installed nvidia driver', level=2)

        if not args.no_driver and need_nvidia_install(args, installed_nvidia):
            run_apt_get_update()
            subprocess.call(maybe_sudo + ["apt-get", "-qq", "install", "dkms", "build-essential"], stdout=apt_stdout, stderr=apt_stdout, env=os.environ)
            log("Disabling nouveau", level=0)
            with open("/etc/modprobe.d/blacklist-nouveau.conf", "w") as nouveau_conf:
                nouveau_conf.write("blacklist nouveau\n")
                nouveau_conf.write("blacklist lbm-nouveau\n")
                nouveau_conf.write("options nouveau modeset=0\n")
            process_check_call(["update-initramfs", "-u"], stderr=logfile, stdout=logfile)

            with yellow():
                print(" * About to download Nvidia driver. You will need to agree to its license to install it.")
            with red():
                print(" * This will reboot your system. Close any open programs, or exit with ctrl+c.")
            with green():
                log("=> Downloading nvidia driver...", level=1)
            nvidia_file = NVIDIA_FILE.format(version=args.driver_version)
            nvidia_dir = NVIDIA_DIR.format(version=args.driver_version)
            if not os.path.exists(nvidia_file) or (driver_hash is not None and get_file_hash(nvidia_file) != driver_hash):
                process_check_call(["wget", "-c", "http://us.download.nvidia.com/XFree86/Linux-x86_64/{version}/NVIDIA-Linux-x86_64-{version}.run".format(version=args.driver_version)])
            with red():
                process_check_call(["chmod", "+x", nvidia_file], stdout=logfile, stderr=logfile)

            if not os.path.isdir(nvidia_dir):
                print(nvidia_file)
                process_call(["/bin/sh", nvidia_file, "-x", "--target", nvidia_dir], stdout=logfile, stderr=logfile)
            with open(os.path.join(nvidia_dir, "LICENSE"), "r") as reader:
                print(reader.read())
            print()
            answer = args.agree_to_nvidia_license
            license_agreement = []
            try:
                while not answer:
                    answer = input(':: Do you agree to this license from Nvidia? [y/N] ').lower().strip() in ["yes", "y"]
                    if not answer:
                        print(" * You must agree to the license to proceed. Press ctrl+c to exit if you do not wish to agree.")
                    if answer:
                        license_agreement = ["--agree-to-nvidia-license"]
            except KeyboardInterrupt:
                shutil.rmtree(nvidia_dir)
                os.unlink(nvidia_file)
                with red():
                    print("\nYou did not agree to the license. Deleted the Nvidia driver.")
                sys.exit(1)
            if not args.launched_in_vt and process_call(["xhost"], stdout=logfile, stderr=logfile, optional=True) == 0:
                process_open(["openvt", "-s", "--", sys.executable] + sys.argv + ["--launched-in-vt"] + license_agreement, stdout=logfile, stderr=logfile)
                sys.exit(0)

            if process_call(["systemctl", "is-active", "--quiet", "lightdm"], stdout=logfile, stderr=logfile) == 0:
                restore_lightdm = True
                process_check_call(["service", "lightdm", "stop"], stdout=logfile, stderr=logfile)
            if "nouveau" in process_check_output(["lsmod"], stderr=logfile):
                for console in glob.glob("/sys/class/vtconsole/vtcon*"):
                    with open(console + "/name", "r") as reader:
                        name = reader.read()
                    if "frame buffer device" not in name:
                        continue
                    with open(console + "/bind", "w") as writer:
                        writer.write("0")
                # don't know if this is needed
                time.sleep(0.2)
                process_check_call(["rmmod", "nouveau"], stdout=logfile, stderr=logfile)

            process_check_call([
                os.path.join(nvidia_dir, "nvidia-installer"),
                "--disable-nouveau",
                "--ui=none",
                "--no-questions",
                "--dkms",
                ],
                               stdout=logfile,
                               stderr=logfile,
                               )
        else:
            with green():
                log("=> Correct nvidia driver version installed", level=1)

        # these four functions from https://askubuntu.com/a/589036

        with green():
            log("=> Install docker", level=0)
        if not args.no_docker:
            run_apt_get_update()
            docker_install(args.allow_partitioning)

        if not args.no_daemon:
            with green():
                log("=> Run Vast.ai daemon", level=0)
            process_check_call(["su", DAEMON_USER, "-c", 'VAST_DEBUG={} bash update_launcher.sh run'.format(str(args.verbose))], stdout=None, stderr=None)

            with green():
                log("=> Wait for daemon to start...", level=0)
            print("\033[37;1mStarting...", end='')
            sys.stdout.flush()
            cycle = 0
            start = time.time()
            progress = 0
            pad = "\r" + " " * 30
            while progress < 30:
                now = time.time()
                progress = now - start
                unwait = max((math.ceil(progress) + start) - now, 0.001)
                time.sleep(unwait)
                try:
                    with open("/var/lib/vastai_kaalia/kaalia.log", "r") as reader:
                        launched = False
                        for line in reader:
                            if "sending" in line:
                                launched = True
                                break
                        if launched:
                            break
                    print(pad+"\rConnecting" + "." * cycle, end="")
                    sys.stdout.flush()
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        print(pad+"\rStarting" + "." * cycle, end="")
                        sys.stdout.flush()
                    else:
                        raise
                cycle += 1
                cycle = cycle % 5
            else:
                print(pad+"\r\033[m\033[33mWaiting timed out")
                raise InstallFailed
            print(pad+"\r\033[m\033[32mDaemon Running")
        with green():
            log("=> Done!", level=0)
        log(level=0)
        log("\033[37;1m    Now go to https://vast.ai/list to set your", level=0)
        log("\033[37;1m    prices and list your machine for rental.", level=0)
        log(level=0)
    upload_logs(success=True)
except SystemExit:
    raise
except BaseException:
    try:
        exc = traceback.format_exc()
        log(exc, level=1)
    finally:
        upload_logs(success=False)
finally:
    if restore_lightdm:
        subprocess.call(["service", "lightdm", "start"], env=os.environ)
