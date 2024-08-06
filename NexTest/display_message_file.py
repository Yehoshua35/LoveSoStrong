import argparse
import sys
from parse_message_file import parse_file, display_services

def main():
    parser = argparse.ArgumentParser(description="Parse and display message file content.")
    parser.add_argument("filename", help="Path to the file to be parsed")
    parser.add_argument("--validate-only", action="store_true", help="Only validate the file without displaying")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose mode")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()

    try:
        if args.validate_only:
            is_valid, error_message, error_line = parse_file(args.filename, validate_only=True, verbose=args.verbose)
            if is_valid:
                print(f"The file '{args.filename}' is valid.")
            else:
                print(f"Validation Error: {error_message}")
                print(f"Line: {error_line.strip()}")
        else:
            services = parse_file(args.filename, verbose=args.verbose)
            if args.debug:
                import pdb; pdb.set_trace()
            display_services(services)
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
