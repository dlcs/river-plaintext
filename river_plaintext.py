from flask import Flask, request, Response, stream_with_context
import settings
import requests
import logging
import json
from concurrent.futures import ThreadPoolExecutor, wait

app = Flask(__name__)


def main():

    app.run(threaded=True, debug=True, port=5050, host='0.0.0.0')


@app.route('/plaintext/', methods=['GET'])
def plaintext():

    manifest_uri = request.args.get('manifestURI')

    manifest_response = requests.get(manifest_uri)
    manifest = json.loads(manifest_response.text)

    return Response(stream_with_context(get_text_for_canvases(manifest)))


def get_text_for_canvases(manifest):

    yield" {\"plaintext\": ["
    for canvas in manifest["sequences"][0]["canvases"]:
        for item in get_text_for_canvas(canvas):
            yield item
    yield "]}"


@app.route('/plaintext_parallel/', methods=['GET'])
def plaintext_parallel():

    manifest_uri = request.args.get('manifestURI')

    manifest_response = requests.get(manifest_uri)
    manifest = json.loads(manifest_response.text)
    canvases = manifest["sequences"][0]["canvases"]

    chunks = [canvases[x:x + settings.CHUNK_SIZE] for x in xrange(0, len(canvases), settings.CHUNK_SIZE)]

    return Response(stream_with_context(process_chunks(chunks)))


def process_chunks(chunks):

    yield " {\"plaintext\": ["
    for chunk in chunks:
        yield get_parallel_text_for_chunk(chunk)  # <--- switch get_parallel_text_for_chunk / get_text_for_chunk
    yield "]}"


def get_parallel_text_for_chunk(chunk):

    # this does in parallel
    chunk_result = ""

    with ThreadPoolExecutor(max_workers=5) as executor:
        for canvas, result in zip(chunk, executor.map(get_text_for_canvas, chunk)):
            chunk_result += result
    return chunk_result


def get_text_for_chunk(chunk):

    # This does in series
    result = ""
    for canvas in chunk:
        result += get_text_for_canvas(canvas)
    return result


def get_text_for_canvas(canvas):

    # assume first image annotating canvas is the one we want
    first_image = next(iter(canvas["images"]))
    if first_image is not None:

        image_uri = first_image["resource"]["service"]["@id"]
        req = requests.get(url=settings.STARSKY, params={"imageURI": image_uri})

        if req.status_code is not 200:
            logging.error("Error obtaining manifest")
            raise IOError("Could not obtain menifest")
        else:
            manifest_string = req.text
            return manifest_string + ","


def set_logging():
    logging.basicConfig(filename="river_plaintext_service.log",
                        filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', )
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)


if __name__ == "__main__":
    main()



