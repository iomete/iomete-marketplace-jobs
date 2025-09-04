#!/usr/bin/env python3
"""
Test script for the DistCp tool
"""

import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from iomete_distcp.distcp import DistCp


@pytest.fixture(scope="module")
def spark_session():
    """Create Spark session for tests"""
    spark = SparkSession.builder.appName("DistCpTest").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def distcp_instance(spark_session):
    """Create DistCp instance for tests"""
    return DistCp(spark_session)


def test_copy_file_success(distcp_instance):
    """Test successful file copying"""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create source file
        source_file = temp_path / "source.txt"
        source_content = "Hello, World!"
        source_file.write_text(source_content)

        # Create destination directory
        dest_dir = temp_path / "dest"
        dest_dir.mkdir()

        # Test copy_file method
        result = distcp_instance.copy_file(
            source_uri=f"file://{temp_path}",
            dest_uri=f"file://{dest_dir}",
            source_path=str(source_file),
        )

        # Verify result
        assert result["status"] == "success"
        assert result["source_path"] == str(source_file)
        assert result["size"] == len(source_content)
        assert "source.txt" in result["dest_path"]

        # Verify file was actually copied
        copied_file = dest_dir / "source.txt"
        assert copied_file.exists()
        assert copied_file.read_text() == source_content


def test_copy_file_source_not_found(distcp_instance):
    """Test copy_file with non-existent source file"""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        dest_dir = temp_path / "dest"
        dest_dir.mkdir()

        # Test with non-existent source
        result = distcp_instance.copy_file(
            source_uri=f"file://{temp_path}",
            dest_uri=f"file://{dest_dir}",
            source_path=str(temp_path / "nonexistent.txt"),
        )

        # Verify failure
        assert result["status"] == "failed"
        assert "error" in result
        assert result["size"] == 0


def test_copy_file_invalid_destination(distcp_instance):
    """Test copy_file with invalid destination"""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create source file
        source_file = temp_path / "source.txt"
        source_file.write_text("test content")

        # Test with invalid destination
        result = distcp_instance.copy_file(
            source_uri=f"file://{temp_path}",
            dest_uri="file:///invalid/path/that/does/not/exist",
            source_path=str(source_file),
        )

        # Verify failure
        assert result["status"] == "failed"
        assert "error" in result
        assert result["size"] == 0


def test_copy_file_from_subdirectory(distcp_instance):
    """Test copy_file with source file in subdirectory"""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create source file in subdirectory
        source_subdir = temp_path / "source" / "nested" / "deep"
        source_subdir.mkdir(parents=True)
        source_file = source_subdir / "nested_file.txt"
        source_content = "Content from nested directory!"
        source_file.write_text(source_content)

        # Create destination directory
        dest_dir = temp_path / "dest"
        dest_dir.mkdir()

        # Test copy_file method
        result = distcp_instance.copy_file(
            source_uri=f"file://{temp_path / 'source'}",
            dest_uri=f"file://{dest_dir}",
            source_path=str(source_file),
        )

        # Verify result
        assert result["status"] == "success"
        assert result["source_path"] == str(source_file)
        assert result["size"] == len(source_content)

        # Verify file was copied with proper relative path structure
        expected_dest = dest_dir / "nested" / "deep" / "nested_file.txt"
        assert expected_dest.exists()
        assert expected_dest.read_text() == source_content


def test_local_copy():
    """Test copying files between local directories"""

    # Create Spark session
    spark = SparkSession.builder.appName("DistCpTest").master("local[2]").getOrCreate()

    try:
        # Setup test directories
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = Path(temp_dir) / "source"
            dest_dir = Path(temp_dir) / "dest"

            source_dir.mkdir()
            dest_dir.mkdir()

            # Create test files
            test_files = [
                ("file1.txt", "Hello World 1"),
                ("file2.txt", "Hello World 2"),
                ("subdir/file3.txt", "Hello World 3"),
            ]

            for file_path, content in test_files:
                full_path = source_dir / file_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                full_path.write_text(content)

            print(f"Created test files in {source_dir}")

            # Run DistCp
            distcp = DistCp(spark)
            distcp.run(f"file://{source_dir}", f"file://{dest_dir}")

            # Verify results - only count files, not directories
            copied_files = [f for f in dest_dir.rglob("*") if f.is_file()]

            assert len(copied_files) == len(
                test_files
            ), "Incorrect number of files copied"
            assert all(
                (dest_dir / file_path).read_text() == content
                for file_path, content in test_files
            ), "Incorrect file contents"
            assert all(
                (dest_dir / file_path).exists() for file_path, _ in test_files
            ), "Missing copied files"

    finally:
        spark.stop()


if __name__ == "__main__":
    test_local_copy()
