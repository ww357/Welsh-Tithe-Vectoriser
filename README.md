
This is a toolkit for comprehensive semi-automated vectorisation of cadastral map series with a built-in feedback loop to iteratively improve prediction capabilities and reduce mending effort over time. Each feature is an individual binary segmentation model, and any combination of features can be selected and compiled from map patch predictions into a GeoPackage of full map vector layers. Land parcels are derived separately, by partitioning the boundary-line network using the recorded apportionment points as seeds (a point-seeded watershed), so no per-parcel model training is required.

The pipeline was initially developed for use with the [Tithe Maps of England and Wales]() to target:
- Solid boundary lines between land parcels with a modified version of [this improved U-Net architecture]() and model weights that have only been trained on tithe map training data (~300 map patches across 12(?) sample maps). Symbology such as building outlines and watercourses that are not land parcel boundaries, yet are morphologically identical, were also annotated to avoid bad prediction from the model's lack of additional semantic knowledge.
- Dashed bounary lines using a custom-made U-Net architecture with a pretrained EfficientNetB0 encoder that learns to redraw dashed boundary lines as if they were continuous using a Gaussian-blurred version of the linear annotation training set. 
- Water (watercourses and waterbodies), land cover symbology (all different symbology together in one class to account for inter-map sheet variability requiring post-prediction manual classification aka filling in an attribute field), and house footprints with [MapSAM](https://arxiv.org/abs/2411.06971). Derived from SAM and fine-tuned for the historic map domain, minimal few-shot fine-tuning is required to produce high-accuracy prediction for each class. 
- Text with the use of the [MapTextPipeline runner](https://github.com/maps-as-data/MapTextPipeline) with model weights built from David Rumsey historical map collection ([add link to these]()). 



Download's and Pre-requisites:
- This pipeline was built in Windows Subsystem for Linux (WSL) because of package and library agreements. 
- The anaconda environments are set up to work best with an NVIDIA GPU.
- Set up the following conda environments:
```python
conda env create -f envs/maptools.yml   # annotation, patchify, and vectorising
conda env create -f envs/polygons.yml   # used for both polygon (MapSAM) and text steps 
conda env create -f envs/lines.yml      # used for both solid and dashed line steps (U-Net models)
Running through the pipeline:
```
- Clone the necessary repositories into the `models/` folder
- Add model weights into the `models/base/` folder from [my huggingface](https://huggingface.co/ww357)

File structure:
``` bash
repo root/
│
├── data/
│   ├── raw/<SHEET>/<SHEET>.tif        ← [YOU] GeoTIFF map scan
│   ├── parcel_points/*.gpkg            ← [YOU] land parcel centroids
│   │
│   ├── map_area_masks/                 (drawn by step 00)
│   ├── patches/                        (created by step 01)
│   ├── annotations/                    (created by step 02)
│   ├── predictions/                    (created by steps 03-04)
│   └── outputs/<SHEET>.gpkg            (final GeoPackage created in step 05)
│
├── models/
│   ├── ImprovedLinearUNet/             ← [YOU] clone from GitHub (https://github.com/ww357/Improved-Linear-U-Net)
│   ├── MapSAM/                         ← [YOU] clone from GitHub (https://github.com/ww357/MapSAM)
│   ├── MapTextPipeline/                ← [YOU] clone from GitHub (https://github.com/maps-as-data/MapTextPipeline)
│   │
│   ├── base/
│   │   ├── unet/model_weights.weights.h5
│   │   ├── MapSAM/origional_weights/sam_vit_b_01ec64.pth  
│   │   └── MapTextPipeline/rumsey-finetune.pth      
│   │
│   └── finetuned/                      (generated model weights will land here in steps 03_finetune and 06_feedback)
│
├── steps/                              (pipeline scripts — do not edit)
├── envs/maptools.yml                   ← use to build the maptools conda env
└── config.yaml                         ← pipeline parameters
```

**Downloading Welsh Tithe maps**:

```python
# first cd into 'steps\00_download'

conda activate maptools

# one-off: catalogue every NLW tithe map (~1,074 maps)
python run.py download discover

# download one or more maps (scan + parcel points + georeferencing)
python run.py download --pids "4634773, 46347"

# export toolkit-ready sheets INTO data/raw and data/parcel_points
python run.py download export-toolkit --county Anglesey

python steps/00_download/tithe_downloader.py --help # for clarity
```

Files in the downloader step:

- `tithe_downloader.py` — the downloader. 
- `steps/00_download/tithe_maps/` Catalogue database and downloaded scans (gitignored). 
- `download.py` — for `run.py` calls.

Per map files:
- `data/raw/<sheet>/<sheet>.tif` — north-up EPSG:27700 GeoTIFF at 0.5 m/px.
- `data/parcel_points/<sheet>_points.gpkg` — apportionment parcel reprojected to EPSG:27700.

The apportionment point layer carries: 
- `rowid` (1..N, the join key the parcel vectorise step uses), `Easting`/`Northing`, the apportionment attributes (`ParishName, ParcelID, Landowner, Occupier, FieldName_Desc, AreaName, CultivationState`)
- the imperial area as separate `Acres` / `Rods` / `Perches` columns
- a computed **`area_hectares`** total, plus `rent_decimal_pounds`, `pixel_x`/`pixel_y`, `map_pid`, `nlw_id`.

To get any downloaded maps into the vectorisation toolkit run:

```python
python run.py download export-toolkit
```
<br>

**Vectorisation options**:

```python
Usage:  python run.py <command> [--sheet SHEET] [flags...]

  00 download
    download             maptools  steps/00_download/download.py

  01 patchify
    reproject            maptools  steps/01_patchify/reproject.py
    draw-mask            maptools  steps/01_patchify/draw_mask.py
    patchify             maptools  steps/01_patchify/patchify.py

  02 annotate
    annotate             maptools  steps/02_annotate/annotate.py
    export-masks         maptools  steps/02_annotate/export_masks.py

  03 finetune
    train-lines          lines     steps/03_finetune/lines/train.py
    train-dashed         lines     steps/03_finetune/dashed/train.py
    dashed-masks         lines     steps/03_finetune/dashed/gaussian_masks.py
    train-polygons       polygons  steps/03_finetune/polygons/train.py
    evaluate-lines       lines     steps/03_finetune/lines/evaluate.py

  04 predict
    predict-lines        lines     steps/04_predict/lines/predict.py
    predict-dashed       lines     steps/04_predict/dashed/predict.py
    predict-polygons     polygons  steps/04_predict/polygons/predict.py
    predict-text         polygons  steps/04_predict/text/predict.py
    predict-parcels      polygons  steps/04_predict/parcels/predict.py

  05 vectorise
    vectorise-lines      maptools  steps/05_vectorise/lines/vectorise.py
    vectorise-dashed     maptools  steps/05_vectorise/dashed/vectorise.py
    vectorise-polygons   maptools  steps/05_vectorise/polygons/vectorise.py
    vectorise-text       maptools  steps/05_vectorise/text/vectorise.py
        polygons  steps/05_vectorise/parcels/vectorise.py

  06 feedback
    prepare-lines        maptools  steps/06_feedback/lines/prepare.py
    feedback-lines       lines     steps/06_feedback/lines/train.py
    prepare-polygons     polygons  steps/06_feedback/polygons/prepare.py
    feedback-polygons    polygons  steps/06_feedback/polygons/train.py

  utility
    fetch-weights        any       steps/fetch_weights.py
    status               any       steps/status.py
```

Random notes:

- draw mask : run this to interactively make mask of map area on the document if necessary
- patchify : slice GeoTIFF into 512px patches (use --mask flag if no mask is auto-found)
- annotate : open labelme to draw boundary lines and feature polygons
- export_mask : convert labelme JSON to binary mask PNGs per feature label



## Acknowledgements

The boundary U-Net architecture is based on:

> Ran et al. (2022). Raster Map Line Element Extraction Method Based on Improved U-Net Network.
> *ISPRS International Journal of Geo-Information*, 11(8), 439.
> https://doi.org/10.3390/ijgi11080439
> GitHub: https://github.com/FutureuserR/Raster-Map

**MapSAM** — feature segmentation model used for buildings, water, and other polygon features.

> Xue Xia, Daiwei Zhang, Wenxuan Song, Wei Huang, Lorenz Hurni.
> *MapSAM: Adapting Segment Anything Model for Automated Feature Detection in Historical Maps.*
> https://github.com/xiaxue-ethz/MapSAM

**MapTextPipeline** — text detection and recognition pipeline used in step 06.

> Based on DNTextSpotter: Yu Xie et al.
> *DNTextSpotter: Arbitrary-Shaped Scene Text Spotting via Improved Denoising Training.*
> arXiv:2408.00355 (2024). https://github.com/yyyyyxie/DNTextSpotter
>
> MapText fork (maps-as-data): https://github.com/maps-as-data/MapTextPipeline

**MapReader** — map patch management and MapTextPipeline runner used in step 06.

> maps-as-data / The Alan Turing Institute.
> https://github.com/maps-as-data/MapReader