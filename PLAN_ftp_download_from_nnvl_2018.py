from ftplib import FTP
import os
import multiprocessing as mp
import itertools
import time


def get_src_dirs(ftp_path):
    with FTP(ftp_path) as ftp:
        ftp.login()
        ftp.cwd('/GOES/GER/')

        src_dirs = set([f for f in ftp.nlst() if f[-4:] == '.jpg'])  # filter
    return src_dirs


def run():
    src_path = 'ftp.nnvl.noaa.gov'
    dst_path = os.path.join('F:/url_get_image', src_path)

    os.mkdir(dst_path) if not os.path.exists(dst_path) else None
    dst_dirs = set([f for f in os.listdir(dst_path) if f[-4:] == '.jpg'])
    src_dirs = get_src_dirs(src_path)
    print("| dst:", len(dst_dirs))
    print("| src:", len(src_dirs))

    src_dirs = src_dirs - dst_dirs  # check local
    print("| src_remain:", len(src_dirs))

    # ftp = FTP(src_path)
    # ftp.login()
    # ftp.cwd('/GOES/GER/')
    #
    # for i, src_dir in enumerate(src_dirs):
    #     argv = (src_path, dst_path, src_dir)
    #     retr_src_dir(argv)
    # ftp.quit()

    # with mp.Pool(8) as pool:
    #     pool.map(retr_src_dir, zip(itertools.repeat(src_path), itertools.repeat(dst_path), src_dirs))

    with mp.Pool(8) as pool:
        pool.map(retr_src_dir, zip(itertools.repeat(src_path), itertools.repeat(dst_path), enumerate(src_dirs)))


def retr_src_dir(argv):
    src_path, dst_path, (i, src_dir) = argv

    with FTP(src_path) as ftp:
        ftp.login()
        ftp.cwd('/GOES/GER/')

        save_path = os.path.join(dst_path, src_dir)
        if not os.path.exists(save_path):
            try:
                ftp.retrbinary('RETR % s' % src_dir, open(save_path, 'wb').write)
                print("| %4d | Save: %s" % (len(os.listdir(dst_path)), src_dir)) if i % 8 == 0 else None
            except Exception as error:
                print("| Error:", error)
    time.sleep(0.01943)


if __name__ == '__main__':
    run()

