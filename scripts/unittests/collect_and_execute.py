import os
import sys

for root, dirs, _files in os.walk('/opt/OpenvStorage'):
    if 'tests' in dirs:
        test_dir = os.path.join(root, 'tests')
        for sub_root, _, sub_files in os.walk(test_dir):
            for file_name in sub_files:
                if 'test' in file_name:
                    test_file = os.path.join(sub_root, file_name)
                    print 'Running {0}'.format(test_file)
                    if file_name == 'test_performance.py':
                        os.system('ovs unittest {0} 20 20'.format(test_file))
                    elif file_name == 'test_arakoonInstaller.py':
                        if len(sys.argv) < 3:
                            print 'Skipping {0} !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'.format(test_file)
                            continue
                        os.system('ovs unittest {0} {1}'.format(test_file, " ".join(sys.argv[1:])))
                    else:
                        os.system('ovs unittest {0}'.format(test_file))
