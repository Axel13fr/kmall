import unittest
import pandas as pd
import os
from KMALL import *

class KmallTest(unittest.TestCase):

    def setUp(self) -> None:
        file_name = "../data/MRZ_LARGE_SIZE.kmall"
        self.f = open(file_name, "rb")
        self.file_size = os.fstat(self.f.fileno()).st_size
        self.k = kmall(file_name)
        self.k.index_file()

        # Panda DataFrame type
        self.index: pd.DataFrame = self.k.Index
        self.mrz_pack = self.index.iloc[0]

    def tearDown(self) -> None:
        self.f.close()

    def test_packet(self):
        self.assertEqual(self.index.shape[0], 1)
        self.assertTrue(self.mrz_pack['MessageSize'] > self.k.MAX_DATAGRAM_SIZE)
        self.assertTrue('#MRZ' in self.mrz_pack['MessageType'])

    def test_raw_header_reading(self):
        header_dict = self.k.read_header_raw(self.f.read(self.k.HEADER_STRUCT_SIZE))
        # Our test file contains only one packet
        self.assertEqual(header_dict['numBytesDgm'], self.file_size)
        self.assertTrue('#MRZ' in str(header_dict['dgmType']))

    def test_partitionning(self):
        msgs = self.k.partition_msg(self.f.read(self.mrz_pack['MessageSize']))
        # Expecting 2 partitions
        self.assertEqual(len(msgs), 2)
        # Let's check the newly generated header content for our splits :
        # First split should be of maximum size
        self.assertEqual(self.k.read_header_raw(msgs[0])['numBytesDgm'], self.k.MAX_DATAGRAM_SIZE)
        # Second and last split should take up the rest
        last_packet_content_size = (self.file_size - self.k.HEADER_AND_PART_SIZE - 4) % self.k.MAX_DATA_SIZE
        last_packet_size = last_packet_content_size + self.k.HEADER_AND_PART_SIZE + 4
        self.assertEqual(self.k.read_header_raw(msgs[1])['numBytesDgm'], last_packet_size)

# Run unit test
if __name__ == '__main__':
    unittest.main()