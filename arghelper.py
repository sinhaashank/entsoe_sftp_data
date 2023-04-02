import os
import argparse

def is_valid_file(arg):
    if not os.path.isfile(arg):
        raise argparse.ArgumentTypeError('The file {} does not exist!'.format(arg))
    else:
        # File exists so return the filename
        return arg


def is_valid_directory(arg):

    if os.path.exists(arg):
        if not os.access(arg, os.W_OK):
            raise argparse.ArgumentTypeError('Directory {} is not writable!'.format(arg))
    else:
        try:
            os.makedirs(arg)
        except Exception as e:
            raise argparse.ArgumentTypeError('Cannot create directory {}!'.format(arg))
    return arg