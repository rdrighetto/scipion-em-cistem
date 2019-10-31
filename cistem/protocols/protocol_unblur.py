# **************************************************************************
# *
# * Authors:     Roberto Marabini (roberto@cnb.csic.es) [1]
# *              Josue Gomez Blanco (josue.gomez-blanco@mcgill.ca) [2]
# *              Grigory Sharov (gsharov@mrc-lmb.cam.ac.uk) [3]
# *
# * [1] Unidad de  Bioinformatica of Centro Nacional de Biotecnologia , CSIC
# * [2] Department of Anatomy and Cell Biology, McGill University
# * [3] MRC Laboratory of Molecular Biology, MRC-LMB
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 2 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
# * 02111-1307  USA
# *
# *  All comments concerning this program package may be sent to the
# *  e-mail address 'scipion@cnb.csic.es'
# *
# **************************************************************************

import os
import time
from itertools import izip
from math import ceil
from threading import Thread

import pyworkflow.utils as pwutils
import pyworkflow.em as em
from pyworkflow.em.data import MovieAlignment
from pyworkflow.protocol import STEPS_PARALLEL
from pyworkflow.em.protocol import ProtAlignMovies
import pyworkflow.protocol.params as params
from pyworkflow.gui.plotter import Plotter

from cistem import Plugin
from cistem.convert import (readShiftsMovieAlignment,
                            writeShiftsMovieAlignment)
from cistem.constants import UNBLUR_BIN


class ProtUnblur(ProtAlignMovies):
    """ This protocol wraps unblur movie alignment program.
    More information at https://cistem.org/documentation#tab-1-6
    """
    _label = 'unblur'
    CONVERT_TO_MRC = 'mrc'

    def __init__(self, **args):
        ProtAlignMovies.__init__(self, **args)
        self.stepsExecutionMode = STEPS_PARALLEL

    def _getConvertExtension(self, filename):
        """ Check whether it is needed to convert to .mrc or not """
        ext = pwutils.getExt(filename).lower()
        return None if ext in ['.mrc', '.mrcs', '.tiff', '.tif'] else 'mrc'

    def _defineAlignmentParams(self, form):
        form.addHidden('doSaveAveMic', params.BooleanParam,
                       default=True)
        form.addHidden('useAlignToSum', params.BooleanParam,
                       default=True)

        group = form.addGroup('Alignment')
        line = group.addLine('Frames to ALIGN',
                             help='Frames range to ALIGN on each movie. The '
                                  'first frame is 1. If you set 0 in the final '
                                  'frame to align, it means that you will '
                                  'align until the last frame of the movie.')
        line.addParam('alignFrame0', params.IntParam, default=1,
                      label='from')
        line.addParam('alignFrameN', params.IntParam, default=0,
                      label='to')

        group.addParam('binFactor', params.FloatParam, default=1.,
                       label='Binning factor',
                       help='1x or 2x. Bin stack before processing.')

        form.addParam('doComputePSD', params.BooleanParam, default=False,
                      expertLevel=params.LEVEL_ADVANCED,
                      label="Compute PSD (before/after)?",
                      help="If Yes, the protocol will compute for each movie "
                           "the average PSD before and after alignment, "
                           "for comparison")
        form.addParam('doComputeMicThumbnail', params.BooleanParam,
                      expertLevel=params.LEVEL_ADVANCED,
                      default=False,
                      label='Compute micrograph thumbnail?',
                      help='When using this option, we will compute a '
                           'micrograph thumbnail and keep it with the '
                           'micrograph object for visualization purposes. ')
        form.addParam('extraProtocolParams', params.StringParam, default='',
                      expertLevel=params.LEVEL_ADVANCED,
                      label='Additional protocol parameters',
                      help="Here you can provide some extra parameters for the "
                           "protocol, not the underlying unblur program."
                           "You can provide many options separated by space. "
                           "\n\n*Options:* \n\n"
                           "--use_worker_thread \n"
                           " Use an extra thread to compute"
                           " PSD and thumbnail. This will allow requires "
                           "an extra CPU. ")

        form.addSection(label='Expert Options')
        line = form.addLine('Shifts (A): ',
                            help='Min and max shifts during alignment.\n\n'
                                 'The minimum shift can be applied '
                                 'during the initial refinement stage. '
                                 'Its purpose is to prevent images aligning '
                                 'to detector artifacts that may be '
                                 'reinforced in the initial sum which is '
                                 'used as the first reference. It is '
                                 'applied only during the first alignment '
                                 'round, and is ignored after that.\n'
                                 'The maximum shift can be applied in any '
                                 'single alignment round. Its purpose is '
                                 'to avoid alignment to spurious noise '
                                 'peaks by not considering unreasonably '
                                 'large shifts.  This limit is applied '
                                 'during every alignment round, but only'
                                 ' for that round, such that it can be '
                                 'exceeded over a number of successive rounds.')
        line.addParam('minShiftInitSearch', params.FloatParam, default='2.0',
                      label='Min shift')
        line.addParam('OutRadShiftLimit', params.FloatParam, default='40.0',
                      label='Max shift')

        group = form.addGroup('Exposure filter')
        group.addParam('doApplyDoseFilter', params.BooleanParam, default=True,
                       label='Exposure filter sums?',
                       help='If selected the resulting aligned movie sums '
                            'will be calculated using the exposure filter '
                            'as described in Grant and Grigorieff (2015). '
                            'Pre-exposure and dose per frame '
                            'should  be specified during movies import.')
        group.addParam('doRestoreNoisePwr', params.BooleanParam,
                       default=True,
                       label='Restore power? ',
                       help='If selected, and the exposure filter is used '
                            'to calculate the sum then the sum will be '
                            'high pass filtered to restore the noise '
                            'power. This is essentially the denominator '
                            'of Eq. 9 in Grant and Grigorieff (2015).')

        group = form.addGroup('Convergence')
        group.addParam('terminShiftThreshold', params.FloatParam,
                       default=1.0,
                       label='Termination threshold (A)',
                       help='The frames will be iteratively aligned '
                            'until either the maximum number of '
                            'iterations is reached, or if after an '
                            'alignment round every frame was shifted '
                            'by less than this threshold.')
        group.addParam('maximumNumberIterations', params.IntParam,
                       default=20,
                       label='Max iterations',
                       help='The maximum number of iterations that '
                            'can be run for the movie alignment. '
                            'If reached, the alignment will stop '
                            'and the current best values will be taken.')

        group = form.addGroup('Filter')
        group.addParam('bfactor', params.FloatParam,
                       default=1500.,
                       label='B-factor (A^2)',
                       help='This B-Factor is applied to the reference sum '
                            'prior to alignment. It is intended to low-pass '
                            'filter the images in order to prevent '
                            'alignment to spurious noise peaks and '
                            'detector artifacts.')

        line = group.addLine('Mask central cross?',
                             help='If selected, the Fourier transform of '
                                  'the reference will be masked by a cross '
                                  'centred on the origin of the transform. '
                                  'This is intended to reduce the influence '
                                  'of detector artifacts which often have '
                                  'considerable power along the central cross.')
        line.addParam('HWHoriFourMask', params.IntParam, default=1,
                      label='Horiz. mask (px)')
        line.addParam('HWVertFourMask', params.IntParam, default=1,
                      label='Vert. mask (px)')

        form.addParallelSection(threads=1, mpi=1)

    #--------------------------- STEPS functions -------------------------------

    def _processMovie(self, movie):
        inputMovies = self.getInputMovies()
        movieFolder = self._getOutputMovieFolder(movie)
        movieBaseName = pwutils.removeExt(movie.getFileName())
        aveMicFn = movieBaseName + '_uncorrected_avg.mrc'
        self._createLink(movie)
        self._argsUnblur(movie)
        
        try:
            self.runJob(self._getProgram(), self._args, cwd=movieFolder,
                        env=Plugin.getEnviron())
            pwutils.moveFile(pwutils.join(movieFolder,
                                          self._getMovieLogFile(movie)),
                             self._getShiftsFn(movie))

            # Compute PSDs
            outMicFn = self._getExtraPath(self._getOutputMicName(movie))
            if not os.path.exists(outMicFn):
                # if only DW mic is saved
                outMicFn = self._getExtraPath(self._getOutputMicWtName(movie))

            def _extraWork():
                if self.doComputePSD:
                    # Compute uncorrected avg mic
                    roi = [0, 0, 0, 0]
                    fakeShiftsFn = self.writeZeroShifts(movie)
                    # FIXME: implement gain flip/rotation
                    self.averageMovie(movie, fakeShiftsFn, aveMicFn,
                                      binFactor=self.binFactor.get(),
                                      roi=roi, dark=inputMovies.getDark(),
                                      gain=inputMovies.getGain())

                    self.computePSDs(movie, aveMicFn, outMicFn,
                                     outputFnCorrected=self._getPsdJpeg(movie))

                self._saveAlignmentPlots(movie, inputMovies.getSamplingRate())

                if self._doComputeMicThumbnail():
                    self.computeThumbnail(outMicFn,
                                          outputFn=self._getOutputMicThumbnail(
                                              movie))
                # This protocols cleans up the temporary movie folder
                # which is required mainly when using a thread for this extra work
                self._cleanMovieFolder(movieFolder)

            if self._useWorkerThread():
                thread = Thread(target=_extraWork)
                thread.start()
            else:
                _extraWork()

        except:
            print("ERROR: Failed to align movie %s\n" % movie.getFileName())

    def _insertFinalSteps(self, deps):
        stepId = self._insertFunctionStep('waitForThreadStep',
                                          prerequisites=deps)
        return [stepId]

    def waitForThreadStep(self):
        # Quick and dirty (maybe desperate) way to wait
        # if the PSD and thumbnail were computed with a thread
        # If running in streaming this will not be necessary
        if self._useWorkerThread():
            time.sleep(60)  # wait 1 min to give some time the thread to finish

    # --------------------------- INFO functions -------------------------------
    def _summary(self):
        summary = []

        if hasattr(self, 'outputMicrographs') or \
                hasattr(self, 'outputMicrographsDoseWeighted'):
            summary.append('Aligned %d movies using unblur.'
                           % self.inputMovies.get().getSize())
        else:
            summary.append('Output is not ready')

        return summary

    def _citations(self):
        return ["Campbell2012", "Grant2015b"]

    def _validate(self):
        # Check base validation before the specific ones
        errors = ProtAlignMovies._validate(self)

        if self.doApplyDoseFilter and self.inputMovies.get():
            inputMovies = self.inputMovies.get()
            doseFrame = inputMovies.getAcquisition().getDosePerFrame()

            if doseFrame == 0.0 or doseFrame is None:
                errors.append('Dose per frame for input movies is 0 or not '
                              'set. You cannot apply dose filter.')

        return errors

    #--------------------------- UTILS functions -------------------------------
    def _getProgram(self):
        return Plugin.getProgram(UNBLUR_BIN)

    def _argsUnblur(self, movie):
        """ Format arguments to call unblur program. """
        inputMovies = self.getInputMovies()
        if self.doApplyDoseFilter:
            preExp, dose = self._getCorrectedDose(inputMovies)
        else:
            preExp, dose = 0.0, 0.0

        args = {'movieName': pwutils.basename(self._getMovieFn(movie)),
                'micFnName': self._getMicFn(movie),
                'shiftsFn': self._getMovieLogFile(movie),
                'samplingRate': self.samplingRate,
                'voltage': movie.getAcquisition().getVoltage(),
                'bfactor': self.bfactor.get(),
                'minShiftInitSearch': self.minShiftInitSearch.get(),
                'OutRadShiftLimit': self.OutRadShiftLimit.get(),
                'HWVertFourMask': self.HWVertFourMask.get(),
                'HWHoriFourMask': self.HWHoriFourMask.get(),
                'terminShiftThreshold': self.terminShiftThreshold.get(),
                'maximumNumberIterations': self.maximumNumberIterations.get(),
                'applyDoseFilter': 'YES' if self.doApplyDoseFilter else 'NO',
                'doRestoreNoisePwr': 'YES' if self.doRestoreNoisePwr else 'NO',
                'exposurePerFrame': dose,
                'binFactor': self.binFactor.get(),
                'alignFrame0': self.alignFrame0.get(),
                'alignFrameN': self.alignFrameN.get(),
                'gainCorrected': 'NO' if inputMovies.getGain() else 'YES',
                'gainFn': inputMovies.getGain(),
                'preExposureAmount': preExp
                }

        argsStr = """ << eof > %(shiftsFn)s
%(movieName)s
%(micFnName)s
%(samplingRate)f
%(binFactor)f
%(applyDoseFilter)s"""

        if self.doApplyDoseFilter:
            argsStr += """
%(voltage)f
%(exposurePerFrame)f
%(preExposureAmount)f"""

        argsStr += """
YES
%(minShiftInitSearch)f
%(OutRadShiftLimit)f
%(bfactor)f
%(HWVertFourMask)d
%(HWHoriFourMask)d
%(terminShiftThreshold)f
%(maximumNumberIterations)d"""

        if self.doApplyDoseFilter:
            argsStr += """
%(doRestoreNoisePwr)s"""

        if inputMovies.getGain():
            argsStr += """
%(gainCorrected)s
%(gainFn)s
%(alignFrame0)d
%(alignFrameN)d
NO
eof\n
"""
        else:
            argsStr += """
%(gainCorrected)s
%(alignFrame0)d
%(alignFrameN)d
NO
eof\n
"""

        self._args = argsStr % args

    def _getMovieFn(self, movie):
        movieFn = movie.getFileName()
        if movieFn.endswith("tiff"):
            return pwutils.replaceExt(movieFn, "tif")
        else:
            return movieFn

    def _createLink(self, movie):
        movieFn = movie.getFileName()
        if movieFn.endswith("tiff"):
            pwutils.createLink(movieFn, self._getMovieFn(movie))

    def _getRelPath(self, baseName, refPath):
        return os.path.relpath(self._getExtraPath(baseName), refPath)

    def _getMicFn(self, movie):
        movieFolder = self._getOutputMovieFolder(movie)
        if self.doApplyDoseFilter:
            return self._getRelPath(self._getOutputMicWtName(movie),
                                    movieFolder)
        else:
            return self._getRelPath(self._getOutputMicName(movie),
                                    movieFolder)

    def _getMovieLogFile(self, movie):
        return 'movie_%06d_shifts.txt' % movie.getObjId()

    def _getShiftsFn(self, movie):
        return self._getExtraPath(self._getMovieRoot(movie) + '_shifts.txt')

    def _getMovieShifts(self, movie):
        """ Returns the x and y shifts for the alignment of this movie. """
        pixSize = movie.getSamplingRate()
        shiftFn = self._getShiftsFn(movie)
        xShifts, yShifts = readShiftsMovieAlignment(shiftFn)
        # convert shifts from Angstroms to px
        xShiftsCorr = [x / pixSize for x in xShifts]
        yShiftsCorr = [y / pixSize for y in yShifts]

        return xShiftsCorr, yShiftsCorr

    def writeZeroShifts(self, movie):
        # TODO: find another way to do this
        shiftsMd = self._getTmpPath('zero_shifts.xmd')
        pwutils.cleanPath(shiftsMd)
        xshifts = [0] * movie.getNumberOfFrames()
        yshifts = xshifts
        alignment = MovieAlignment(first=1, last=movie.getNumberOfFrames(),
                                   xshifts=xshifts, yshifts=yshifts)
        roiList = [0, 0, 0, 0]
        alignment.setRoi(roiList)
        movie.setAlignment(alignment)
        writeShiftsMovieAlignment(movie, shiftsMd,
                                  1, movie.getNumberOfFrames())
        return shiftsMd

    def _doComputeMicThumbnail(self):
        return self.doComputeMicThumbnail.get()

    def _preprocessOutputMicrograph(self, mic, movie):
        mic.plotGlobal = em.Image(location=self._getPlotGlobal(movie))
        if self.doComputePSD:
            mic.psdCorr = em.Image(location=self._getPsdCorr(movie))
            mic.psdJpeg = em.Image(location=self._getPsdJpeg(movie))
        if self._doComputeMicThumbnail():
            mic.thumbnail = em.Image(
                location=self._getOutputMicThumbnail(movie))

    def _getNameExt(self, movie, postFix, ext, extra=False):
        fn = self._getMovieRoot(movie) + postFix + '.' + ext
        return self._getExtraPath(fn) if extra else fn

    def _getPlotGlobal(self, movie):
        return self._getNameExt(movie, '_global_shifts', 'png', extra=True)

    def _getPsdCorr(self, movie):
        return self._getNameExt(movie, '_psd_comparison', 'psd', extra=True)

    def _getPsdJpeg(self, movie):
        return self._getNameExt(movie, '_psd', 'jpeg', extra=True)

    def _saveAlignmentPlots(self, movie, pixSize):
        """ Compute alignment shift plots and save to file as png images. """
        shiftsX, shiftsY = self._getMovieShifts(movie)
        first, _ = self._getFrameRange(movie.getNumberOfFrames(), 'align')
        plotter = createGlobalAlignmentPlot(shiftsX, shiftsY, first, pixSize)
        plotter.savefig(self._getPlotGlobal(movie))
        plotter.close()

    def _useWorkerThread(self):
        return '--use_worker_thread' in self.extraProtocolParams.get()

    def getInputMovies(self):
        return self.inputMovies.get()

    def _createOutputMicrographs(self):
        return not self.doApplyDoseFilter

    def _createOutputWeightedMicrographs(self):
        return self.doApplyDoseFilter


def createGlobalAlignmentPlot(meanX, meanY, first, pixSize):
    """ Create a plotter with the shift per frame. """
    sumMeanX = []
    sumMeanY = []

    def px_to_ang(ax_px):
        y1, y2 = ax_px.get_ylim()
        x1, x2 = ax_px.get_xlim()
        ax_ang2.set_ylim(y1*pixSize, y2*pixSize)
        ax_ang.set_xlim(x1*pixSize, x2*pixSize)
        ax_ang.figure.canvas.draw()
        ax_ang2.figure.canvas.draw()

    figureSize = (6, 4)
    plotter = Plotter(*figureSize)
    figure = plotter.getFigure()
    ax_px = figure.add_subplot(111)
    ax_px.grid()
    ax_px.set_xlabel('Shift x (px)')
    ax_px.set_ylabel('Shift y (px)')

    ax_ang = ax_px.twiny()
    ax_ang.set_xlabel('Shift x (A)')
    ax_ang2 = ax_px.twinx()
    ax_ang2.set_ylabel('Shift y (A)')

    i = first
    # by the program if dose filtering is required
    skipLabels = ceil(len(meanX)/10.0)
    labelTick = 1

    for x, y in izip(meanX, meanY):
        sumMeanX.append(x)
        sumMeanY.append(y)
        if labelTick == 1:
            ax_px.text(x - 0.02, y + 0.02, str(i))
            labelTick = skipLabels
        else:
            labelTick -= 1
        i += 1

    # automatically update lim of ax_ang when lim of ax_px changes.
    ax_px.callbacks.connect("ylim_changed", px_to_ang)
    ax_px.callbacks.connect("xlim_changed", px_to_ang)

    ax_px.plot(sumMeanX, sumMeanY, color='b')
    ax_px.plot(sumMeanX, sumMeanY, 'yo')
    ax_px.plot(sumMeanX[0], sumMeanY[0], 'ro', markersize=10, linewidth=0.5)
    #ax_ang2.set_title('Full-frame alignment')

    plotter.tightLayout()

    return plotter
